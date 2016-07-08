'''
Created on May 23, 2016

@author: abhinav
'''
import gevent


### node ###
# node = table(node_id , client_id, addr , addr_internal , port,  cluster_id , is_external_node, current_connections, max_concurrent_cnnections)
# connection = table(connection_id, from_node_id, to_node_id)


### messaging IM ###
# messages = table(cliend_id, payload, time_stamp)
# messaging_meta = table(session_id, messages_count)


# group = table(group_id, name, description) 
# group_clients = table(group_id , client_id)

### temporary session like multiplayer games , just subscription 
# session =  table( session_id , name , description , created_by_client_id)
# session_nodes - table(session_id , client_id)



# from oauth2client.service_account import ServiceAccountCredentials 
# scopes = ['https://www.googleapis.com/auth/userinfo.email' 'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/datastore']
# 
# service_account_json_file_name = 'Samosa-Uploads-OAuth-Key.json'
# credentials = ServiceAccountCredentials.from_json_keyfile_name(
#                                                                service_account_json_file_name,
#                                                                 scopes=scopes)
# 
# from google.appengine.ext.remote_api import remote_api_stub
# remote_api_stub.ConfigureRemoteApiForOAuth(
#         '{}.appspot.com'.format("the-tasty-samosa"),
#         '/_ah/remote_api')

try:
    import dev_appserver
    dev_appserver.fix_sys_path()
except ImportError:
    print('Please make sure the App Engine SDK is in your PYTHONPATH.')
    raise


email = '259129283998-f4acgtan2q32bt00pn9ovu33cb4m62n4@developer.gserviceaccount.com'

from google.appengine.ext.remote_api import remote_api_stub
remote_api_stub.ConfigureRemoteApiForOAuth(
     '{}.appspot.com'.format("the-tasty-samosa"),
     '/_ah/remote_api' ,
     service_account=email, 
     key_file_path='Samosa-1927925e9abc.p12')


import util_funcs
from google.appengine.ext import ndb
from lru_cache import LRUCache
from models_ndb import NodeEntity, ConnectionEntity, SessionNodesEntity,\
    SessionEntity
from datetime import datetime
from models.users import UserInboxMessage
from logger import logger


class Db():

    node_cache = LRUCache(10000) #lets say for fun c10k

    def init(self):
        self.ndb_transactions_queue = []
        pass
    
    def do_in_background(self):
        while(True):
            while(len(self.ndb_transactions_queue)>0):
                try:
                    self.ndb_transactions_queue.pop(0).get_result()
                except:
                    logger.error("An error occured in db..")
            
            gevent.sleep(5)
            
            
    def get_node_by_id(self, node_id, strict_check=True):
        node = self.node_cache.get(node_id)
        if(node):
            return node
        
        
        node = NodeEntity.get_by_id(node_id)        
        if (not node):
            #such node never existed
            if (strict_check):
                return None
            else:
                node = NodeEntity(id=node_id, node_id=node_id)
                self.ndb_transactions_queue.append(node.put_async())
                
        ret =  node.to_dict() # returns a dict object
        ret["node_id"] = node.key.id()
        self.node_cache.set(node_id, ret)
        return ret

    def update_android_gcm_key(self, node_id, android_gcm_key):
        node = NodeEntity.get_by_id(node_id)
        node.gcm_key = android_gcm_key
        self.ndb_transactions_queue.append(node.put_async())
        #update cache
        ret =  node.to_dict() # returns a dict object
        ret["node_id"] = node.key.id()
        
        self.node_cache.set(node_id, ret)
        return True
    

    def get_node_with_connection_to(self, node_id):
        ret = []
        query = ConnectionEntity.query(#ndb.OR(ConnectionEntity.from_node_key==ndb.Key(NodeEntity, node_id),
                                       ConnectionEntity.to_node_key==ndb.Key(NodeEntity, node_id))
                                    #)
        connections = query.fetch()
        for conn in connections:
            ret.append(conn.to_node_key.id() if conn.from_node_key.id() == node_id else conn.from_node_key.id())
        return ret[0] if ret else None

    # check in this function if you want to limit creating more nodes
    def check_and_add_new_connection(self, connection_id, node_id1, node_id2):
        if (not connection_id):
            return self.add_connection(node_id1, node_id2)
        else:
            # check if connection_id exists and return connection_id else None
            return connection_id

    def is_server_node(self, node_id):
        node = self.get_node_by_id(node_id)
        return node and node.get('addr', None) != None

    def add_connection(self, node_id1, node_id2):
        connection_id = util_funcs.get_random_id(10)
        # TODO: check if already exists
        conn = ConnectionEntity(id=connection_id, connection_id=connection_id , from_node_key = ndb.Key(NodeEntity, node_id1),
                                to_node_key = ndb.Key(NodeEntity, node_id2))
        self.ndb_transactions_queue.append(conn.put_async())
        return connection_id

    def remove_connection(self, connection_id):
        key = ndb.Key(ConnectionEntity, connection_id)
        self.ndb_transactions_queue.append(key.delete_async())
        
        
    def get_node_ids_by_client_id(self, client_id):
        query = NodeEntity.query(NodeEntity.client_id==client_id)
        nodes = query.fetch()
        ret = []
        for node in nodes:
            ret.append(node.key.id())
        return ret

    def get_node_ids_for_session(self, session_id):
        ret = []
        query = SessionNodesEntity.query(SessionNodesEntity.session_key==ndb.Key(SessionEntity, session_id))
        session_nodes = query.fetch()
        for i in session_nodes:
            ret.append(i.node_key.id())
        return ret

    def create_node(self, client_id, addr, addr_internal, port):
        node_id = ((client_id + "__") if client_id else "") + util_funcs.get_random_id(10)
        node = NodeEntity(id=node_id, node_id=node_id , client_id=client_id, addr = addr, addr_internal=addr_internal, port=port)
        self.ndb_transactions_queue.append(node.put_async())
        return node_id
    
    
#     def is_valid_node_fwd(self, node_id1, node_id2):
#         # todo , should be memcached
#         node1 = self.get_node_by_id(node_id1)
#         node2 = self.get_node_by_id(node_id2)
# 
#         client_id1 = node1["client_id"]
#         client_id2 = node2["client_id"]
#         if (not client_id1 or not client_id2):
#             return True
# 
#         if (client_id1 == client_id2):
#             return True
# 
#         return self.is_clients_connected(client_id1, client_id2)
# 
#     def is_clients_connected(self, client_id1, client_id2):
#         client_connection = self.client_network.find_one({"client_id1": client_id1, "client_id2": client_id2})
#         return client_connection["direction"] != 0
    
    def node_config_exists(self, addr, port):
        query = NodeEntity.query(NodeEntity.addr==addr, NodeEntity.port==port)
        node = query.get()
        if (node):
            return node.key.id()
        return None

    def clear_connections_to_node_from_db(self, node_id):
        PAGE_SIZE = 200
        more = True
        cursor = None
        query =  ConnectionEntity.query(ConnectionEntity.to_node_key==ndb.Key(NodeEntity, node_id))
        
        while(more):
            entities, cursor, more = query.fetch_page(PAGE_SIZE,\
                                                          start_cursor=cursor,\
                                                          produce_cursors=True,\
                                                          keys_only=True)
            count  = len(entities)
            ndb.delete_multi(entities)
            print "deleted connections:"+str(count)

    def create_session(self, name, description, client_id):
        session_id = util_funcs.get_random_id(10)
        session = SessionEntity(id=session_id, session_id=session_id,  name=name, description=description, client_id=client_id)
        self.ndb_transactions_queue.append(session.put_async())
        return session_id

    def join_session(self, session_id, node_id):
        session_node = SessionNodesEntity(id=session_id + "__" + node_id, session_key=ndb.Key(SessionEntity, session_id),
                                          node_key=ndb.Key(NodeEntity, node_id))
        self.ndb_transactions_queue.append(session_node.put_async())

#     def remove_client_nodes(self, client_id):
#         self.nodes.remove({"client_id": client_id})
                    
    def add_pending_messages(self, node_id, message_type, message, current_timestamp=0):
        created_at = datetime.utcfromtimestamp(current_timestamp/1000.0)
        return UserInboxMessage.add_inbox_message(ndb.Key('UserEntity', node_id), message_type, message , created_at = created_at)
            
    def fetch_inbox_messages(self, node_id , from_seq=-1, to_seq = -1,  last_message_seen_timestamp=None):   
        last_message_seen_time = datetime.utcfromtimestamp(last_message_seen_timestamp/1000.0)
        user_inbox_messages , from_seq, to_seq, more =  UserInboxMessage.get_inbox_messages(ndb.Key('UserEntity', node_id), from_seq, to_seq, last_message_seen_time)
        return map(lambda inbox_message: inbox_message.message_payload , user_inbox_messages), from_seq, to_seq, more



if __name__ == '__main__':
    db = Db()
    print db.get_node_by_id("some_random_node", strict_check=False)
