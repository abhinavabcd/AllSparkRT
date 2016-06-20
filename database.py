'''
Created on May 23, 2016

@author: abhinav
'''


from pymongo import MongoClient
import config
from bson.objectid import ObjectId
import util_funcs
from lru_cache import LRUCache

### node ###
# node = table(node_id , client_id, addr , addr_internal , port,  cluster_id , is_external_node, current_connections, max_concurrent_cnnections)
# connection = table(connection_id, from_node_id, to_node_id)


### messaging IM ###
# messages = table(cliend_id, payload, time_stamp)
# messaging_meta = table(session_id, messages_count)


# client_network = table(client_id1, client_id2, direction) # direction = -1 -> negetive , 1 -> possitive, 2 -> bidirectional


# group = table(group_id, name, description) 
# group_clients = table(group_id , client_id)

### temporary session like multiplayer games , just subscription 
# session =  table( session_id , name , description , created_by_client_id)
# session_nodes - table(session_id , client_id)



class Db():
    db = None
    connections  = None
    nodes  = None
    
    
    node_cache = LRUCache(10000)
    def init(self):
        client = MongoClient('mongodb://localhost:27017/')
        self.db = client[config.DB_NAME]
        self.nodes = self.db["nodes"]
        self.connections = self.db["connections"]
        
        self.sessions = self.db["sessions"]
        self.session_nodes = self.db["session_nodes"]
        
        self.group = self.db["group"]
        
        self.client_network = self.db["client_network"]
        
        
    
    # should return  a dict 
    def get_node_by_id(self, node_id, strict_check = True):
        node = self.node_cache.get(node_id)
        if(not node):
            node = self.nodes.find_one({"node_id":node_id})
            if(node): self.node_cache.set(node_id, node)
        if(not node):
            if(strict_check):
                return None
            else:
                #create one and return
                inserted_one = self.nodes.insert_one({"node_id":node_id})
                return {"node_id":node_id} # some anonymous connection
        return node
            
            
    def update_android_gcm_key(self, node_id, android_gcm_key):
        result = self.nodes.update_one({"node_id":node_id}, {"gcm_key": android_gcm_key})
        return result.modified_count ==1
    
    
    def get_node_with_connection_to(self , node_id):
        ret = []
        for conn in self.connections.find({"$or":[{"to_node_id":node_id}, {"from_node_id":node_id}]}):
            ret.append(conn['to_node_id'] if conn['from_node_id']==node_id else conn['from_node_id'])
        return ret[0] if ret else None
    
    
    
    # check in this function if you want to limit creating more nodes
    def check_and_add_new_connection(self, connection_id, node_id1, node_id2):
        if(not connection_id):
            return self.add_connection(node_id1, node_id2)
        else:
            #check if connection_id exists and return connection_id else None
            return connection_id
    
    def is_server_node(self, node_id):
        node = self.node_cache.get(node_id)
        if(not node):
            node = self.get_node_by_id(node_id)
            self.node_cache.set(node_id , node)
        return node and node.get('addr',None)!=None
    
    
    def add_connection(self, node_id1 , node_id2):
        connection_id = util_funcs.get_random_id(10)
        #TODO: check if already exists
        conn = self.connections.insert_one({"connection_id":connection_id , "from_node_id":node_id1, "to_node_id":node_id2})
        return connection_id
    
    
    def remove_connection(self, connection_id):
        self.connections.remove({"connection_id":connection_id})
    
    
    def get_node_ids_by_client_id(self, client_id):
        nodes = self.nodes.find({"client_id":client_id})
        ret  = []
        for node in nodes:
            ret.append(node["node_id"])
        return ret
    

    def get_node_ids_for_group(self, group_id):
        client_ids = []
        for i in self.groups.find({"group_id":group_id}):
            client_ids.append(i["client_id"])
        ret = []
        for client_id in client_ids:
            ret.append(self.get_node_ids_by_client_id(client_id))
        
    
    def get_node_ids_for_session(self, session_id):
        ret = []
        for i in self.session_nodes.find({"session_id":session_id}):
            ret.append(i["node_id"])
        return ret

    def create_node(self, client_id , addr , addr_internal, port, is_server=False):
        node_id = ((client_id+"__") if client_id else "")+util_funcs.get_random_id(10)
        if(is_server):
            node_id = "server__"+node_id
            
        self.nodes.insert_one({"node_id":node_id, "client_id":client_id ,"addr":addr, "addr_internal":addr_internal, "port":port})
        return  node_id

    def is_valid_node_fwd(self, node_id1, node_id2):
        # todo , should be memcached
        node1 = self.get_node_by_id(node_id1)
        node2 = self.get_node_by_id(node_id2)
        
        client_id1 = node1["client_id"]
        client_id2 = node2["client_id"]
        if(not client_id1 or  not client_id2):
            return True
        
        if(client_id1 == client_id2):
            return True
        
        return self.is_clients_connected(client_id1, client_id2)
    
    def is_clients_connected(self, client_id1, client_id2):
        client_connection = self.client_network.find_one({"client_id1":client_id1, "client_id2":client_id2})
        return client_connection["direction"]!=0
    
    
    def node_config_exists(self, addr, port):
        node =  self.nodes.find_one({"addr":addr , "port":port})
        if(node):
            return node["node_id"]
        return None
    
    def clear_connections_to_node_from_db(self, node_id):
        pass


    
    def create_session(self, name, description , client_id):
        session_id = util_funcs.get_random_id(10)
        self.sessions.insert_one({"session_id":session_id, "name":name, "description":description, "client_id":client_id })
        return session_id
    
    
    def join_session(self, session_id , node_id):
        self.session_nodes.insert_one({"session_id":session_id, "node_id":node_id})
        
    def remove_client_nodes(self, client_id):
        self.nodes.remove({"client_id":client_id})
    
    def add_pending_messages(self, node_id, message):
        pass
        
    def fetch_inbox_messages(self, node_id , from_seq=-1, to_seq=-1, last_message_seen_time=None):
        return None, None, None # return messages as strings , until_sequence_number , last_sequence_number
