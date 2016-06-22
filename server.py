'''
Created on May 22, 2016

@author: abhinav


connection_id is used to notify on the both ends a connection has broken, double ensure

node_id is unique id we assign to every device/installation and is a secret , should be always encrypted

Nodes are the servers/clients
connections are edges between them , we can put stuff into the connection queue


1) During connection an auth is sent this, contains from_node and connection_id


'''
import gevent
from gevent import monkey
import util_funcs
import sys
from datetime import date, datetime
from time import sleep
import urllib
from urllib_utils import get_data
monkey.patch_all()


from websocket import create_connection , WebSocket


from gevent.lock import BoundedSemaphore
from geventwebsocket.server import WebSocketServer
from geventwebsocket.resource import Resource, WebSocketApplication





from logger import logger
import urlparse
from collections import OrderedDict
import config

import re
import cookies
from bson import json_util

from database_ndb import Db


db = Db()



current_node = None

MONOCAST_DIRECTLY = 1
BROADCAST_ALL = 2


    
class Connection(WebSocket):
    queue = []# to keep track of how many greenlets are waiting on semaphore to send 
    ws = None
    from_node_id = None
    to_node_id = None
    lock = BoundedSemaphore()
    is_stale = False
    connection_id = None
    is_external_node = False    
    last_msg_recv_time = None
    
    def __init__(self, ws,  to_node_id, client_id , connection_id):
        self.ws = ws
        self.to_node_id = to_node_id
        self.client_id = client_id
        self.connection_id = connection_id
        self.last_msg_recv_time =  datetime.now()
        if(not connection_id):# mean we are getting this from an unknown one sided party
            self.is_external_node = True
        
        
    def send(self, msg): # msg is only string data
        self.queue.append(msg)
        self.lock.acquire()
        try:
            while(not self.is_stale and len(self.queue)>0):
                data = self.queue.pop(0)
                self.ws.send(data) # msg objects only
        except:
            logger.debug("Error occured while sending, closing connection")
            if(not self.is_stale):
                current_node.destroy_connection(self.ws)
            self.is_stale = True
            
        self.lock.release()
        return not self.is_stale
            
        
    def on_close(self):
        self.unregister()
    
class Message():
    #encrypted client_id
    
    dest_id = None
    dest_client_id = None
    dest_session_id = None
    
    src_client_id = None
    src_id = None
    
    
    type = None
    payload = None
    payload1 = None
    payload2 = None
    id = None
    is_ack_required = True
    
    timestamp = None # utc time stamp
    
    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])
            
    def to_son(self):
        ret = self.__dict__
        for k in ret.keys():
            if(ret[k]==None):
                del ret[k]                
        return ret

class Node():
    
    cluster_id = None
    
    client_id = None
    node_id =  None
    addr = None
    addr_internal = None
    port = None
    ssl_enabled = None
    
    
    connections = {} # node_id -> list of connection to it
    connections_ws = {} # ws - > connection_obj
    
    
    def send_heartbeat(self):
        ping_json = json_util.dumps(Message(src_id=self.node_id).to_son())
        while(True):
            last_ping_sent = datetime.now()
            for node_id in self.connections.keys():
                for conn in self.connections.get(node_id , []):
                    try:
                        conn.send(ping_json)
                    except:
                        pass
            time_elapsed = (datetime.now() - last_ping_sent).total_seconds()
            logger.debug("sent a heartbeat")
            gevent.sleep(max(0 , 30*60 - (time_elapsed))) # 10 minutes send a heart beat
            
            
            
    @classmethod
    def get_connection_info(cls, auth_key=None):
        data = cookies.decode_signed_value(config.SERVER_SECRET, config.SERVER_AUTH_KEY_STRING, urllib.unquote(auth_key))
        try:
            data = json_util.loads(data)
            return data.get("node_id",None), data.get("connection_id",None)
        except:
            return data, None
        
    @classmethod
    def get_connection_auth_key(cls, node_id, connection_id):
        return cookies.create_signed_value(config.SERVER_SECRET, config.SERVER_AUTH_KEY_STRING  , json_util.dumps({"node_id":node_id, "connection_id":connection_id}))
    
    def get_connection(self,  node_id):#return physical connection object to forward the message to
        
        conn_list = self.connections.get(node_id)# get direct connection to node_id if exists
        if(conn_list):
            for conn in conn_list:
                if len(conn.queue)<50:
                    return conn
        
        is_server = db.is_server_node(node_id)                
        if(not is_server):#not reachable directly, we mean we cannot open connection to that
            if(conn_list):# just return whatever connection we have to that client
                return conn_list[0] # althogh the queue size is high , we will reuse it , as we cannot make new connection to client directly
            
            #check for any intermediate node that it is connected to
            intermediate_node_id = db.get_node_with_connection_to(node_id)
            if(intermediate_node_id==None or intermediate_node_id==current_node.node_id):
                return None
            return self.get_connection(intermediate_node_id)
        else:
            # try making connection to the server
            
            c = 3
            while(c>0):
                try:                          
                    conn =  self.make_new_connection(node_id)
                    return conn
                except Exception as e:
                    logger.error(sys.exc_info())
                    pass
                c-=1
            return None
        
    #called by the underlying websockets
    def on_new_connection(self, ws, from_node, connection_id):
        
        from_node_id = from_node.node_id
        
        client_id = from_node.client_id
        
        logger.debug("New connection from "+ from_node_id)
        conn = Connection(ws, from_node_id, client_id, connection_id)
        if(not connection_id):#anonymous connection
            conn.is_external_node = True
            
        conn.connection_id  = connection_id = db.check_and_add_new_connection(connection_id , from_node_id , current_node.node_id)
        if(connection_id):
            temp = self.connections.get(from_node_id, None)
            if(not temp):
                temp = []
                self.connections[from_node_id] = temp
            temp.append(conn)
            self.connections_ws[ws] = conn
            return conn
        else:
            ws.close()
            return None
        
    #use this to forward to a multiple client nodes or , a direct node , kjust like normal communication
    def on_message(self, ws, msg, msg_obj = None): # msg is string , msg_obj is Message object
        
        ### setting the src id if connection is anonymous , making sure it's not tampered
        ### A better solution is to use another auth_key for src_id and don't tamper        
        
        
        msg = msg_obj or Message(**json_util.loads(msg))
        
        from_conn = None
        if(ws):
            from_conn = self.connections_ws[ws]
            from_conn.last_msg_recv_time = datetime.now()
            if(from_conn.is_external_node):# set the src if only if from external_nodefor delivery reports
                msg.src_id = from_conn.to_node_id
                msg.src_client_id = from_conn.client_id
                msg.timestamp = util_funcs.toUtcTimestamp(datetime.now())
   
        logger.debug("message recieved from "+msg.src_id)
        
        dest_ids = []
        if(msg.dest_id):# single node
            dest_ids = [msg.dest_id]
                
        elif(msg.dest_client_id):# broadcast to every client node if a destination is not specified
            dest_ids = db.get_node_ids_by_client_id(msg.dest_client_id)
                
        elif(msg.dest_session_id):
            dest_id = db.get_node_ids_for_session(msg.session_id)
            
        else:# should have atleast one destination set
            if(msg.type==-101):
                #config message
                #update gcm key from client
                logger.debug("recieved config message from: "+from_conn.to_node_id)
                
                user_service_request = json_util.loads(msg.payload)
                update_gcm_key = user_service_request.get('update_gcm_key',None)
                fetch_inbox_messages = user_service_request.get('fetch_inbox_messages',None)
                
                
                
                if(update_gcm_key):
                    logger.debug("updating gcm key: "+ update_gcm_key +" "+from_conn.to_node_id)
                    db.update_android_gcm_key(msg.src_id, update_gcm_key)
                
                if(fetch_inbox_messages):
                    fetch_inbox_messages_from_seq = user_service_request.get("fetch_inbox_messages_from_seq",-1)
                    fetch_inbox_messages_to_seq = user_service_request.get("fetch_inbox_messages_to_seq",-1)
                    fetch_inbox_messages_from_timestamp = user_service_request.get("fetch_inbox_messages_from_time_stamp",None)
                    if(fetch_inbox_messages_from_timestamp):
                        fetch_inbox_messages_from_timestamp = datetime.fromtimestamp(fetch_inbox_messages_from_timestamp)
                    messages, from_seq , to_seq, has_more = db.fetch_inbox_messages(msg.src_id, fetch_inbox_messages_from_seq, fetch_inbox_messages_to_seq, fetch_inbox_messages_from_timestamp)
                    payload = json_util.dumps({"messages":messages, "from_seq":from_seq, "to_seq":to_seq, "more":has_more})
                    from_conn.send(json_util.dumps(Message(type=-102, payload=payload).to_son()))
            return
        
#         if(db.is_valid_node_fwd(msg.src_id, msg.dest_id)):
        
        for dest_id in dest_ids:# sending to all nodes
            if(not dest_id or dest_id==msg.src_id): continue
            
            while(True):#loops through connections until you can send the message it once
                conn = self.get_connection(dest_id)
                if(not conn): 
                    logger.debug("Could not send, putting into db, and notifying user about new messages")
                    db.add_pending_messages(dest_id, msg.type, json_util.dumps(msg.to_son()))
                    self.send_a_ting(dest_id)
                    #send a push notification to open and fetch any pending messages
                    break # cannot find any connection
                try:
                    if((datetime.now() - conn.last_msg_recv_time).total_seconds()>20*60):#20 minutes no ping
                        raise Exception("Not ping recieved , stale connection")#probably a stale connection 
                    
                    msg.dest_id = dest_id
                    conn.send(json_util.dumps(msg.to_son())) # this could raise 
                    logger.debug("message sent to "+dest_id)
                    break# successfully forwarded
                except Exception as e:
                    #keep it in db to send it later
                    logger.error(sys.exc_info())
                    self.destroy_connection(conn.ws)
                    logger.debug("An exception occured while sending, retrying with another connection")
                    
          
    
    def send_a_ting(self, dest_id):
        #put to a queue
        node = db.get_node_by_id(dest_id)
        if(not node):
            logger.debug("node not yet registered")
            return
        gcm_key = node.get("gcm_key", None)
        if(gcm_key):
            logger.debug("sending a push notification")
            GCM_HEADERS ={'Content-Type':'application/json',
                          'Authorization':'key='+config.GCM_API_KEY 
                         }
            
            packetData={"message":"You have pending messages",
                        "notification_type": 101
                        }
            registrationIds =[
                              gcm_key
            ]
            data = {"registration_ids":registrationIds,"data":packetData }
                        
            post= json_util.dumps(data)
            headers = GCM_HEADERS
            ret=get_data('https://android.googleapis.com/gcm/send',post,headers).read()
            
            
    
    
    #### make and destroy connection, called knowingly whenever needed
    def make_new_connection(self, to_node_id):
        def recv_from_ws(ws, cb_msg, cb_close):
            try:
                while(True):
                    data = ws.recv()
                    if(not data):
                        cb_close(self, ws)
                        return
                    cb_msg(self, ws, data)
            except:
                cb_close(self, ws)
        

        to_node = util_funcs.from_kwargs(Node, **db.get_node_by_id(to_node_id))
        connection_id = db.add_connection(current_node.node_id, to_node_id)
        try:
            ws = create_connection("ws://"+to_node.addr+":"+to_node.port+"/connect?auth_key="+Node.get_connection_auth_key(current_node.node_id, connection_id))
            ws.settimeout(5)
            conn = self.on_new_connection(ws, to_node, connection_id)
            #TODO: keep recieving for on close may be ?
            gevent.spawn(recv_from_ws , ws, Node.on_message, Node.destroy_connection)
            return conn
        
        except:
            #logger.error(sys.exc_info())
            db.remove_connection(connection_id)
        return None
    
    
    def destroy_connection(self, ws):
        conn = self.connections_ws.get(ws,None)
        if(not conn): return
        del self.connections_ws[ws]
        
        try:
            ws.close()
        except Exception as ex:
            pass

        
        db.remove_connection(conn.connection_id)
        
        current_node.connections.get(conn.to_node_id).remove(conn)
        logger.debug(current_node.node_id+": destroying "+ conn.connection_id+ " with node "+ conn.to_node_id)
                
   

class Paths(Resource):
    def __init__(self, apps=None):
        Resource.__init__(self, apps=apps)
        
    def _app_by_path(self, environ_path, is_websocket_request):
        # Which app matched the current path?
        for path, app in self.apps.items():
            match = re.match(path, environ_path)
            if match:
                if is_websocket_request == self._is_websocket_app(app):
                    return app , match.groups()
        return None , None

    def __call__(self, environ, start_response):
        environ = environ
        is_websocket_call = 'wsgi.websocket' in environ
        path_info = environ['PATH_INFO']
        query_params = environ.get('QUERY_STRING',None)
        if(query_params):
            query_params = urlparse.parse_qs(query_params)
            
        current_app, args = self._app_by_path(path_info, is_websocket_call)

        if current_app is None:
            raise Exception("No apps defined")

        if is_websocket_call:
            ws = environ['wsgi.websocket']
            current_app = current_app(ws, *args, query_params=query_params)
            current_app.handle()
            # Always return something, calling WSGI middleware may rely on it
            return []
        else:
            return current_app(start_response, *args, query_params=query_params)



class InstaKnow(WebSocketApplication):
    query_params = None
    ws = None
    def __init__(self, ws, query_params=None):                
        self.query_params = query_params
        self.ws = ws
        super(InstaKnow, self).__init__(ws)
                
    def on_open(self, *args, **kwargs):        
        #TODO: query_params["auth_key"];
        auth_key = self.query_params.get("auth_key",None)
        if(not auth_key):
            self.on_close(None)
            return
        auth_key = auth_key[0]
        from_node_id , connection_id  = Node.get_connection_info(auth_key)

        from_node = util_funcs.from_kwargs(Node, **db.get_node_by_id(from_node_id, strict_check=False))
        if(self.query_params.get("get_pre_connection_info",None)):
            client_id = from_node.client_id
            session_id = self.query_params.get("session_id", None)
            # based on client_id , session_id , send a node details to connect to
            if(session_id or client_id or True):
                #TODO: logically decide a node use has to connect to
                self.ws.send(json_util.dumps(current_node.__dict__))
                
            self.ws.close()
                        
        else:
            self.ws.stream.handler.socket.settimeout(5)
            current_node.on_new_connection(self.ws, from_node , connection_id)
    
    def on_close(self, reason):
        current_node.destroy_connection(self.ws)
        
    def on_message(self, msg):
        if(msg!=None):
            current_node.on_message(self.ws, msg)
            
        

def start_transport_server():
    global current_node
    db.init()        
    
    import argparse

    parser = argparse.ArgumentParser(description='process arguments')
    parser.add_argument('--host_address',
                       help='host name specifically')
    
    parser.add_argument('--port',
                       help='host port specifically')
    
    parser.add_argument('--force',
                       help='force use same node config')
    

    args = parser.parse_args()
    
    if(not args.port or not  args.host_address):
        logger.debug("port and host name needed")
        return
    
    node_id = db.node_config_exists(args.host_address, args.port)
    if(node_id and not args.force):
        logger.error("Node config exists in db")
        return
    
    node_id = node_id or db.create_node(None, args.host_address, None, args.port)
    current_node = util_funcs.from_kwargs(Node, **db.get_node_by_id(node_id))
    
    ## clear all connections to the node from db 
    db.clear_connections_to_node_from_db(node_id)
    
    
    thread = gevent.spawn(current_node.send_heartbeat)# loop forever and send heartbeat every 10 minutes
    
    WebSocketServer(
    ('0.0.0.0', int(args.port)),

    Paths(OrderedDict([
        ('^/connectV2', InstaKnow), # /app/app_id is the websocket path
    ])),

    debug=False
    ).serve_forever()
    

if __name__ =="__main__":
    start_transport_server()
    
