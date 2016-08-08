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
import collections
from ws_server import WebSocketServerHandler
from socket import errno
import socket
import struct
import logging
monkey.patch_all()
import util_funcs
import sys
from datetime import date, datetime, timedelta
import time
import urllib
from urllib_utils import get_data
from config import EPOCH_DATETIME
from gevent.server import StreamServer

from websocket import create_connection , WebSocket


from gevent.lock import BoundedSemaphore



##message types
CLIENT_CONFIG_REQUEST = -101
CLIENT_CONFIG_RESPONSE = -102
USER_OFFLINE_RESPONSE = -4

NEW_NODE_JOINED_SESSION = 100
NODE_UNJOINED_SESSION = 103

### 




CONNECT_PATH = "/connectV2"

from logger import logger, log_handler , init_timed_rotating_log
import urlparse
from collections import OrderedDict

import re
import cookies
from bson import json_util

from database_mongo import Db


import config

db = Db()

max_assumed_sent_buffer_time = 100*1000 #milli seconds # TCP_USER_TIMEOUT kernel setting

current_node = None
request_handlers = []
MONOCAST_DIRECTLY = 1
BROADCAST_ALL = 2


    
class Connection(WebSocket):
    queue = None# to keep track of how many greenlets are waiting on semaphore to send 
    msg_assumed_sent = None# queue for older sent messages in case of reset we try to retransmit
    ws = None
    from_node_id = None
    to_node_id = None
    lock = BoundedSemaphore()
    is_stale = False
    connection_id = None
    is_external_node = False
    last_msg_recv_timestamp = None
    last_msg_sent_timestamp = None
    
    
    def __init__(self, ws,  to_node_id, client_id , connection_id):
        self.ws = ws
        self.queue = collections.deque()# to keep track of how many greenlets are waiting on semaphore to send 
        self.msg_assumed_sent = collections.deque()# queue for older sent messages in case of reset we try to retransmit
   
   
        self.to_node_id = to_node_id
        self.client_id = client_id
        self.connection_id = connection_id
        self.last_msg_recv_timestamp = self.last_msg_sent_timestamp = time.time()*1000
        if(not connection_id):# mean we are getting this from an unknown one sided party
            self.is_external_node = True
        
        
    def send(self, msg, ref=None): # msg is only string data , #ref is used , just in case an exception occurs , we pass that ref 
        if(self.is_stale):
            raise Exception("stale connection")
        
        self.queue.append((ref, msg))
        if(self.lock.locked()):
            return
        
        self.lock.acquire()
        data_ref = None
        data = None
        try:
            while(not self.is_stale and len(self.queue)>0):
                data_ref, data = self.queue.popleft() #peek
                self.ws.send(data) # msg objects only
                current_timestamp = time.time()*1000
                self.last_msg_sent_timestamp = current_timestamp
                
                while(len(self.msg_assumed_sent)>0 and self.msg_assumed_sent[0][0]<current_timestamp-max_assumed_sent_buffer_time):
                    #keep inly 100 seconds of previous data
                    self.msg_assumed_sent.popleft()
                
                self.msg_assumed_sent.append((current_timestamp , data_ref, data))
                
#                logger.debug("message sent to "+self.to_node_id)
            
        except  Exception as ex:
            err_msg  = "Exception while sending message to %s , might be closed "%self.to_node_id
            logger.debug(err_msg)
            self.is_stale = True
            raise Exception(err_msg)
            
        finally:
            self.lock.release()
        return 
            
        
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
        if(not self.timestamp):
            self.timestamp = int(time.time()*1000)
            
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
    
    _msg_recieved_counter = 0
    
    connections = {} # node_id -> list of connection to it
    connections_ws = {} # ws - > connection_obj
    
    
    _delta_connections = 0
    _update_on_num_connections_change = 100
    
    def send_heartbeat(self):
        ping_json = json_util.dumps({"src_id":self.node_id})
        while(True):
            last_ping_sent = time.time()
            for node_id in self.connections.keys():
                to_destroy = []
                node_connections = self.connections.get(node_id , [])
                for conn in node_connections:
                    if(conn.is_external_node):
                        try:
                            conn.send(ping_json)
                        except:
                            to_destroy.append(conn)
                    else:
                        if(time.time()*1000 - conn.last_msg_recv_timestamp > 30*60*1000 and time.time()*1000 - conn.last_msg_sent_timestamp > 30*60*1000):
                            #30 min no msg received or sent, basically very idle
                            to_destroy.append(conn)
                             
                
                for conn in to_destroy:
                    self.destroy_connection(conn.ws, conn_obj=conn)
                            
            time_elapsed = time.time()-last_ping_sent
            logger.debug("sent a heartbeat")
            gevent.sleep(max(0 , 10*60 - (time_elapsed))) # 10 minutes send a heart beat
            
    
    def refresh_stats(self):
        while(True):
            self.update_node_info()
            gevent.sleep(config.UPDATE_STATS_INTERVAL)#every 5 minutes
    
    def update_node_info(self):
        logger.debug("Updating node stats num_connections : %d "%len(self.connections_ws))
        db.update_node_info(self.node_id, num_connections = len(self.connections_ws), num_msg_transfered=self._msg_recieved_counter)
        self._msg_recieved_counter  = 0
        
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
                    logger.debug("using connection for %s %s"%(node_id, conn.connection_id))
                    return conn
        
        is_server = db.is_server_node(node_id)
        if(is_server==None):
            return None
        if(not is_server):#not reachable directly, we mean we cannot open connection to that
            if(conn_list):# just return whatever connection we have to that client
                conn = conn_list[0] # althogh the queue size is high , we will reuse it , as we cannot make new connection to client directly
                logger.debug("using connection for %s %s"%(node_id, conn.connection_id))
                return conn
                
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
                    node = db.get_node_by_id(node_id)
                    if(node.get("cluster_id", None)!=self.cluster_id):
                        #check if there is an existing connection to that cluster
                        pass
                        
                                          
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
        
        conn = Connection(ws, from_node_id, client_id, connection_id)
        if(not connection_id):#anonymous connection
            conn.is_external_node = True
            
        connection_id = db.check_and_add_new_connection(connection_id , from_node_id , current_node.node_id)
        conn.connection_id = connection_id
        
        logger.debug("New connection from "+ from_node_id+ " connection_id :: "+connection_id)
        
        self.connections_delta_changed(1)
        
        if(connection_id):
            temp = self.connections.get(from_node_id, None)
            if(not temp):
                temp = []
                self.connections[from_node_id] = temp
            temp.append(conn)
            self.connections_ws[ws] = conn
            
            ping_json = '{"src_id":"'+self.node_id+'"}'
            for prev_conn in temp:#send ping to see if any previous connections are alive
                if(conn!=prev_conn):
                    try:
                        prev_conn.send(ping_json)
                    except:
                        self.destroy_connection(prev_conn.ws, conn_obj=prev_conn)
                    
            return conn
        else:
            ws.close()
            return None
    
    def connections_delta_changed(self, num):
        self._delta_connections+=num
        if(self._delta_connections>self._update_on_num_connections_change):
            self._delta_connections = 0
            gevent.spawn(self.update_node_info)

    
    #use this to forward to a multiple client nodes or , a direct node , kjust like normal communication
    def on_message(self, ws, msg, msg_obj = None): # msg is string , msg_obj is Message object
        
        ### setting the src id if connection is anonymous , making sure it's not tampered
        ### A better solution is to use another auth_key for src_id and don't tamper        
        
        
        if(msg_obj):
            msg = msg_obj
        else:
            if(len(msg)>1*1000*1000):#1M bytes
                if(ws):
                    from_conn = self.connections_ws[ws]
                    print from_conn.to_node_id
                    self.destroy_connection(ws)
                print "##uploaded greater than 1 MB "
                return 
            msg = Message(**json_util.loads(msg))
        
        from_conn = None
        current_timestamp = time.time()*1000
        if(ws):#if no websocket, internal transfer only
            from_conn = self.connections_ws[ws]
            from_conn.last_msg_recv_timestamp = current_timestamp
            if(from_conn.is_external_node):# set the src if only if from external_nodefor delivery reports
                msg.src_id = from_conn.to_node_id
                msg.src_client_id = from_conn.client_id
                msg.timestamp = int(current_timestamp) # millis
   
        
        dest_ids = []
        if(msg.dest_id):# single node
            dest_ids = [msg.dest_id]
                
        elif(msg.dest_client_id):# broadcast to every client node if a destination is not specified
            dest_ids = db.get_node_ids_by_client_id(msg.dest_client_id)
                
        elif(msg.dest_session_id):
            dest_ids = db.get_node_ids_for_session(msg.session_id)
            
        else:# should have atleast one destination set
            if(msg.type==CLIENT_CONFIG_REQUEST):
                #config message
                #update gcm key from client
                logger.debug("recieved config message from: "+from_conn.to_node_id)
                
                user_service_request = json_util.loads(msg.payload)
                update_gcm_key = user_service_request.get('update_gcm_key',None)
                fetch_inbox_messages = user_service_request.get('fetch_inbox_messages',None)
                
                
                user_time_stamp = user_service_request.get("timestamp", msg.timestamp)
                if(update_gcm_key):
                    logger.debug("updating gcm key: "+ update_gcm_key +" "+from_conn.to_node_id)
                    db.update_android_gcm_key(msg.src_id, update_gcm_key)
                
                if(fetch_inbox_messages):
                    fetch_inbox_messages_from_seq = user_service_request.get("fetch_inbox_messages_from_seq",-1)
                    fetch_inbox_messages_to_seq = user_service_request.get("fetch_inbox_messages_to_seq",-1)
                    fetch_inbox_messages_from_timestamp = user_service_request.get("fetch_inbox_messages_from_time_stamp",0)
                    
                    messages, from_seq , to_seq, has_more = db.fetch_inbox_messages(msg.src_id, fetch_inbox_messages_from_seq, fetch_inbox_messages_to_seq, fetch_inbox_messages_from_timestamp)
                    payload = json_util.dumps({"messages":messages, "from_seq":from_seq, "to_seq":to_seq, "more":has_more, "server_timestamp_add_diff":int(current_timestamp-user_time_stamp)})
                    try:
                        from_conn.send(json_util.dumps(Message(type=CLIENT_CONFIG_RESPONSE, payload=payload, dest_id=from_conn.to_node_id).to_son()))
                    except:
                        self.destroy_connection(from_conn.ws, conn_obj=from_conn)
            return
        
        if(msg.type == NEW_NODE_JOINED_SESSION):
            #update in our cache too
            # this message is sent as a broadcast to all users of a session
            update_in_db = False
            if(from_conn and from_conn.is_external_node):
                update_in_db  = True#this is the primary node the new node_id is connected to and requested to join the session
            db.join_session(msg.dest_session_id, msg.src_id, update_in_db = update_in_db)
            
            
        if(msg.type == NODE_UNJOINED_SESSION):
            update_in_db = False
            if(from_conn and from_conn.is_external_node):
                update_in_db  = True#this is the primary node the new node_id is connected to and requested to unjoin the session
            db.unjoin_session(msg.dest_session_id, msg.src_id, update_in_db = update_in_db)
            
            
        if(dest_ids):
            self._msg_recieved_counter+=1
            if(msg.src_id):
                logger.debug("message recieved from "+msg.src_id)            
            
        
#         if(db.is_valid_node_fwd(msg.src_id, msg.dest_id)):
        
        for dest_id in dest_ids:# sending to all nodes
            if(not dest_id or dest_id==msg.src_id): continue
            
            while(True):#loops through connections until you can send the message it once
                conn = self.get_connection(dest_id)
                msg.dest_id = dest_id
                if(not conn): 
                    logger.debug("Could not send, putting into db, and notifying user about new messages")
                    if(msg.type==-102 or msg.type==0 or msg.type==None or msg.type==USER_OFFLINE_RESPONSE):
                        break# no need to insert into db , pings and config messages
                    
                    if(not from_conn):
                        from_conn = self.get_connection(msg.src_id)
                    
                    if(from_conn):
                        try:
                            from_conn.send(json_util.dumps(Message(type=USER_OFFLINE_RESPONSE, src_id=msg.dest_id).to_son()))
                        except:
                            logger.debug("wtf!!! connection closed !! %s"%msg.src_id)
                    db.add_pending_messages(msg.dest_id, msg.type, json_util.dumps(msg.to_son()), current_timestamp)
                    self.send_a_ting(dest_id, msg)
                    #send a push notification to open and fetch any pending messages
                    break # cannot find any connection
                try:
                    if(current_timestamp - conn.last_msg_recv_timestamp > 30*60*1000):#20 minutes no ping
                        raise Exception("Not ping recieved , stale connection")#probably a stale connection 
                    conn.send(json_util.dumps(msg.to_son()) , ref=msg) # this could raise 
                    break# successfully forwarded to node
                except Exception as e:
                    #keep it in db to send it later
                    self.destroy_connection(conn.ws, conn_obj=conn)
                    logger.debug("An exception occured while sending, retrying with another connection")
                    
          
    
    def send_a_ting(self, dest_id , msg=None):
        #put to a queue
        node = db.get_node_by_id(dest_id)
        if(not node):
            logger.debug("node not yet registered")
            
            msg = '{"dest_id":"'+msg.src_id+'", "type":-3, "src_id":"'+dest_id+'"}'
            self.on_message(None, msg)
            return
        
        
        gcm_key = node.get("gcm_key", None)
        if(gcm_key and node.get("last_push_sent", config.EPOCH_DATETIME)+timedelta(minutes=10)<datetime.now()):
            node["last_push_sent"] = datetime.now()
            logger.debug("sending a push notification")
            GCM_HEADERS ={'Content-Type':'application/json',
                          'Authorization':'key='+config.GCM_API_KEY 
                         }
            
            title = "You have pending messages"
            if(msg and msg.type==1):
                title = msg.payload
                
            if(msg and msg.type==2):
                title = "Sent you a poke"
                
                
            packetData={"message":title,
                        "payload1":"" if not msg else msg.src_id,
                        "notification_type": 101
                        }
            registrationIds =[
                              gcm_key
            ]
            data = {"registration_ids":registrationIds,"data":packetData }
            logger.debug(registrationIds)
            post= json_util.dumps(data)
            headers = GCM_HEADERS
            ret=get_data('https://android.googleapis.com/gcm/send',post,headers).read()
            logger.debug(ret)
        else:
            logger.debug("too soon to send another push notification")
            
    
    
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
            conn_addr = to_node.addr
            if(to_node.addr == current_node.addr):
                conn_addr = "127.0.0.1" # loopback
                
            ws = create_connection("ws://"+conn_addr+":"+to_node.port+CONNECT_PATH+"?auth_key="+Node.get_connection_auth_key(current_node.node_id, connection_id))
            #ws.settimeout(5)
            conn = self.on_new_connection(ws, to_node, connection_id)
            #TODO: keep recieving for on close may be ?
            gevent.spawn(recv_from_ws , ws, Node.on_message, Node.destroy_connection)
            return conn
        
        except:
            #logger.error(sys.exc_info())
            db.remove_connection(connection_id)
        return None
    
    
    def destroy_connection(self, ws, conn_obj=None, on_exception=None):
#         for line in traceback.format_stack():
#             print(line.strip())
        resend_last_msgs = False
        if(on_exception):
            logger.debug("destroying on exception:  "+str(on_exception))
            resend_last_msgs=(type(on_exception)==socket.error)
        
        conn = self.connections_ws.pop(ws,None)
        if(not conn):
            if(not conn_obj):# no connection object passed
                return
            conn = conn_obj
        
        db.remove_connection(conn.connection_id)
        try:
            self.connections.get(conn.to_node_id).remove(conn)
        except:
            logger.debug("strange error , connection not in node connections list")
        self.connections_delta_changed(-1)
        
        # retransmit messages onto other connections for this node
        while(len(conn.queue)>0):
            #these are all assumsed sent , so , insert them into db as pending messages
            logger.debug("Could not send, retrying with another connections")
            ref , data = conn.queue.popleft()
            self.on_message(None, data, msg_obj= (ref if (type(ref) is Message) else None))
        
        while(resend_last_msgs and len(conn.msg_assumed_sent)>0):
            timestamp , ref , data = conn.msg_assumed_sent.popleft()
            if(ref and (type(ref) is Message) and not (ref.type==-102 or ref.type==0 or ref.type==None or ref.type==USER_OFFLINE_RESPONSE)):
                logger.debug("resending from the last buffer")
                self.on_message(None, data, msg_obj=ref)
        
        
        logger.debug(current_node.node_id+": destroying "+ conn.connection_id+ " with node "+ conn.to_node_id)
        try:
            ws.close()
        except Exception as ex:
            pass
                
   
######websocket handling

#new greenlet





def set_socket_options(sock):

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    l_onoff = 1                                                                                                                                                           
    l_linger = 10 # seconds,                                                                                                                                                     
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,                                                                                                                     
                 struct.pack('ii', l_onoff, l_linger))# close means a close understand ? 
    

    TCP_USER_TIMEOUT = 18
    max_unacknowledged_timeout = 10*1000 #ms                                                                                                                                           
#    sock.setsockopt(socket.SOL_TCP, TCP_USER_TIMEOUT, max_unacknowledged_timeout)# close means a close understand ? 




def websocket_handler(sock, query_params=None, headers= None):
    
    auth_key = query_params.get("auth_key",None)
    if(not auth_key):#need auth_key for sure
        sock.close()
        return
    
    auth_key = auth_key[0]
    from_node_id , connection_id  = Node.get_connection_info(auth_key)
    if(not from_node_id):
        sock.close()
        return
            
    node_obj = db.get_node_by_id(from_node_id, strict_check=False)
    node_obj["last_push_sent"] = EPOCH_DATETIME
    from_node = util_funcs.from_kwargs(Node, **node_obj)
    
    

    if(query_params.get("get_pre_connection_info",None)):
        client_id = from_node.client_id
        session_id = query_params.get("session_id", None)
        if(session_id):
            session_id = session_id[0]
        need_80_port = query_params.get("need_80_port", None)
        if(need_80_port):
            need_80_port = True
            
        # based on client_id , session_id , send a node details to connect to
        if(session_id or client_id or True):
            node = db.get_a_connection_node(need_80_port=need_80_port)
            #TODO: logically decide a node use has to connect to
            if(headers.get("Sec-WebSocket-Key", None)):#through websocket
                    ws = WebSocketServerHandler(sock, headers)#sends handshake automatically 
                    ws.do_handshake(headers)
                    ws.send(json_util.dumps(node))
                    ws.close()
            else:
                write_data(sock, "HTTP/1.1 200 OK\r\n\r\n")
                write_data(sock, json_util.dumps(node))
                sock.close()
                        
    else:
#           self.ws.stream.handler.socket.settimeout(5)
        set_socket_options(sock)

        ws = WebSocketServerHandler(sock, headers)#sends handshake automatically 
        ws.handleClose = lambda ex: current_node.destroy_connection(ws, on_exception=ex)
        ws.handleConnected = lambda : current_node.on_new_connection(ws, from_node, connection_id)
        ws.handleMessage = lambda : current_node.on_message(ws, ws.data)
        
        ws.do_handshake(headers)
        ws.start_handling()
    





########inner functionality

def write_data(socket, data):
    n = 0
    l = len(data)
    while(n<l):
        sent = socket.send(data[n:])
        n += sent
        if(sent<0):
            break


def read_line(socket):
    data = ""
    while(True):
        byt = socket.recv(1)
        data+=byt
        if(byt=='\n' or not byt):
            return data

def handle_connection(socket, address): 
    request_line = read_line(socket)
    request_params = {}
    try:
        request_type , request_path , http_version = request_line.split(" ")
        query_start_index = request_path.find("?")
        if(query_start_index!=-1):
            request_params = urlparse.parse_qs(request_path[query_start_index+1:])
            request_path = request_path[:query_start_index]
            
    except:
        socket.close()
        
    logger.debug("new request" +  request_line)
    headers = {}
    while(True):
        l = read_line(socket)
        if(l=='\r\n'):
            break
        if( not l):
            return
        header_type , data  =  l.split(": ",1)
        headers[header_type] = data
        
    if(request_type == "POST" and headers.get("Content-Length", None)):
        n = int(headers.get("Content-Length","0").strip(" \r\n"))
        if(n>0):
            data = ""
            while(len(data) < n):
                bts = socket.recv(n)
                if(not bts):
                    break
                data +=bts
            if(request_params):
                request_params.update(urlparse.parse_qs(data))
            else:
                request_params = urlparse.parse_qs(data)
    ##app specific headers
            
    for handler in request_handlers:
        
        args = handler[0].match(request_path)
        func = handler[1]
        kwargs = {}
        kwargs["query_params"] = request_params
        kwargs["headers"] = headers
        
        if(args!=None):
            fargs = args.groups()
            if(fargs):
                func(socket, *fargs , **kwargs)
                return
            else:
                func(socket, **kwargs)
                return

        

def start_transport_server(handlers=[]):
    global current_node
    global request_handlers
    db.init(user_name="", password="", host="localhost", namespace="instaknow")
        
    import argparse

    parser = argparse.ArgumentParser(description='process arguments')
    parser.add_argument('--host_address',
                       help='host name specifically')
    
    parser.add_argument('--port',
                       help='host port specifically')
    
    parser.add_argument('--force',
                       help='force use same node config')
    
    parser.add_argument('--proxy_80_port',
                       help='proxy server to connect to this server')
    
    parser.add_argument('--log_level',
                       help='log level , debug or error or info')
    

    args = parser.parse_args()
    
    if(args.log_level=='debug'):
        logger.setLevel(logging.DEBUG)
        log_handler.setLevel(logging.DEBUG)
        #init_timed_rotating_log("logs/logs_"+args.port+".log",  logging.DEBUG)
    
    if(not args.port or not  args.host_address):
        logger.debug("port and host name needed")
        return
    
    node_id = db.node_config_exists(args.host_address, args.port)
    if(node_id and not args.force):
        logger.error("Node config exists in db")
        return
    
    
    node_id = node_id or db.create_node(None, args.host_address, None, args.port)

    db.update_node_info(node_id, proxy_80_port= args.proxy_80_port , num_connections=0, num_max_connections=1400)
    
    current_node = util_funcs.from_kwargs(Node, **db.get_node_by_id(node_id))
    
    ## clear all connections to the node from db 
    db.clear_connections_to_node_from_db(node_id)
    
    
    thread = gevent.spawn(current_node.send_heartbeat)# loop forever and send heartbeat every 10 minutes
    
    refresh_stats  = gevent.spawn(current_node.refresh_stats)# loop forever and send heartbeat every 10 minutes
    db_periodic_flush = gevent.spawn(db.do_in_background)# loop forever and send heartbeat every 10 minutes
    
   
    for regex, handler in handlers:
        if(isinstance(regex, str)):
            regex = re.compile(regex)
        request_handlers.append((regex, handler))
    
    
    request_handlers.sort(key = lambda x:x[0] , reverse=True)
    
    server = StreamServer(
    ('', int(args.port)), handle_connection)
    
    
    server.serve_forever()    

if __name__ =="__main__":
    start_transport_server([('^/connectV2', websocket_handler)])
    
