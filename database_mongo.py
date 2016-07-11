'''
Created on May 23, 2016

@author: abhinav
'''


from pymongo import MongoClient
import config
import util_funcs
from lru_cache import LRUCache
import gevent
from logger import logger
import time
import pymongo

### node ###
# node = table(node_id , client_id, addr , addr_internal , port,  cluster_id , is_external_node, current_connections, max_concurrent_cnnections)
# connection = table(connection_id, from_node_id, to_node_id)

### temporary session like multiplayer games , or groups 
# session =  table( session_id , name , description , created_by_client_id)
# session_nodes - table(session_id , client_id)

### pending messages (node_id_seq_number, message_type, message_json, timestamp)
### node_seq_number ( node_id , seq_number , timestamp)

class Db():
    db = None
    connections  = None
    nodes  = None
    
    
    node_cache = LRUCache(10000)
    user_seq_cache = LRUCache(10000)
    def init(self):
        client = MongoClient("mongodb://127.0.0.1/")
        self.db = client[config.DB_NAME]
        self.nodes = self.db["nodes"]
        self.connections = self.db["connections"]        
        self.sessions = self.db["sessions"]
        self.session_nodes = self.db["session_nodes"]
        
        self.node_seq = self.db["node_seq"]
        self.pending_messages = self.db["pending_messages"]
            
            
            
    def do_in_background(self):
        while(True):
            #do some flushing
            gevent.sleep(5)
        
    
    # should return  a dict 
    def get_node_by_id(self, node_id, strict_check = True):
        node = self.node_cache.get(node_id)
        if(node):
            return node
        
        node = self.nodes.find_one({"node_id":node_id})
        if(not node):
            if(strict_check):
                return None
            else:
                #create one and return
                node  = {"node_id":node_id}
                inserted_id = self.nodes.insert_one(node)
                
        self.node_cache.set(node_id, node)
        return node
            

    def update_node_info(self, node_id, proxy_80_port=None,  num_connections=-1, num_max_connections=-1, num_msg_transfered=-1):
        u = {"$set":{}}
        if(num_connections!=-1):
            u["$set"]["num_connections"] =  num_connections
        if(num_max_connections!=-1):
            u["$set"]["num_max_connections"] =  num_max_connections 
        if(num_msg_transfered!=-1):
            if(not u.get("$inc",None)):
                u["$inc"] = {}
            u["$inc"]["num_msg_transfered"] =  num_msg_transfered
            u["$set"]["num_msg_transfer_rate"] =  num_msg_transfered       
        
        if(proxy_80_port!=None):
            u["$set"]["proxy_80_port"] =  proxy_80_port
        
            
        
        u["$set"]["last_stats_refresh_timestamp"] = int(time.time())
        self.nodes.update_one({"node_id":node_id} , u)
        
        
    def get_a_connection_node(self, need_80_port=False):
        query = {"addr": {"$ne": None} , "last_stats_refresh_timestamp" :{"$gt" : int(time.time()) - 5*60 +1  }, "$where": "this.num_connections < this.num_max_connections" }
        if(need_80_port):
            query["proxy_80_port"] = {"proxy_80_port": {"$ne":None}}
            
        nodes = self.nodes.find().sort([("_id",1)])
        for i in nodes:
            return i

        if(need_80_port):#but we couldn't find
            return self.get_a_connection_node(need_80_port=False)
        
        return self.nodes.find({})[0]

            
    def update_android_gcm_key(self, node_id, android_gcm_key):
        result = self.nodes.update_one({"node_id":node_id}, {"$set": {"gcm_key": android_gcm_key}})
        node = self.node_cache.get(node_id)
        if(node):
            node["gcm_key"] = android_gcm_key
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
        node = self.get_node_by_id(node_id)
        if(not node):
            return None
        return node.get('addr',None)!=None
    
    
    def add_connection(self, node_id1 , node_id2):
        connection_id = util_funcs.get_random_id(10)
        #TODO: check if already exists
        conn = self.connections.insert_one({"connection_id":connection_id , "from_node_id":node_id1, "to_node_id":node_id2})
        return connection_id
    
    
    def remove_connection(self, connection_id):
        self.connections.delete_one({"connection_id":connection_id})
    
    
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

#     def is_valid_node_fwd(self, node_id1, node_id2):
#         # todo , should be memcached
#         node1 = self.get_node_by_id(node_id1)
#         node2 = self.get_node_by_id(node_id2)
#         
#         client_id1 = node1["client_id"]
#         client_id2 = node2["client_id"]
#         if(not client_id1 or  not client_id2):
#             return True
#         
#         if(client_id1 == client_id2):
#             return True
#         
#         return self.is_clients_connected(client_id1, client_id2)
#     
#     def is_clients_connected(self, client_id1, client_id2):
#         client_connection = self.client_network.find_one({"client_id1":client_id1, "client_id2":client_id2})
#         return client_connection["direction"]!=0
    
    
    def node_config_exists(self, addr, port):
        node =  self.nodes.find_one({"addr":addr , "port":port})
        if(node):
            return node["node_id"]
        return None
    
    def clear_connections_to_node_from_db(self, node_id):
        result = self.connections.delete_many({"to_node_id":node_id})
        logger.debug("deleted connections from db : "+str(result.deleted_count))
    
    def create_session(self, name, description , client_id):
        session_id = util_funcs.get_random_id(10)   
        self.sessions.insert_one({"session_id":session_id, "name":name, "description":description, "client_id":client_id })
        return session_id
    
    
    def join_session(self, session_id , node_id):
        result = self.session_nodes.insert_one({"session_id":session_id, "node_id":node_id})
        return result.inserted_id!=None
    
    
    def remove_client_nodes(self, client_id):
        result = self.nodes.delete_many({"client_id":client_id})
    
    
    
    def add_pending_messages(self, node_id, message_type, message_json, current_timestamp=None):
        
        seq = self.get_seq(node_id)
        if(not current_timestamp):
            current_timestamp = int(time.time()*1000)
            
        self.pending_messages.insert_one({"node_id_seq":node_id+"__"+str(seq), "message_type":message_type, "message_json":message_json, "timestamp":current_timestamp})
            
    def fetch_inbox_messages(self, node_id , from_seq=-1, to_seq = -1,  last_message_seen_time=None):   
        if(to_seq==-1):
            to_seq = self.get_seq(node_id)
        if(from_seq==-1):
            from_seq = max(0 , to_seq - 50)-1
            
        ret = []
        flag = False
        more  = False
        for i in range(to_seq, from_seq, -1):
            for j in self.pending_messages.find({"node_id_seq": node_id+"__"+str(i) , "timestamp": {"$gt":last_message_seen_time}}).sort([("timestamp", pymongo.DESCENDING)]):
                ret.append(j)
                if(len(ret)>50):
                    flag = True
                    more = True
                    break
            if(flag):
                break
        return map(lambda x: x["message_json"] ,  ret), from_seq, to_seq, more
                    
            


    def get_seq(self, node_id):
        
        user_seq = self.user_seq_cache.get(node_id)
        if(not user_seq):
            user_seq = self.node_seq.find_one({"node_id":node_id})
            if(user_seq!=None):
                self.user_seq_cache.set(node_id, user_seq )
            
        if(user_seq):
            ret = user_seq["seq"]
            current_timestamp = time.time()*1000
            if(current_timestamp - user_seq["timestamp"] >30*60*1000):
                ret+=1
                user_seq["timestamp"] = current_timestamp
                user_seq["seq"]+=1
                self.node_seq.update_one({"node_id":node_id}, {"$inc":{"seq":1} , "$set": {"timestamp": current_timestamp}})              
            return ret
        else:
            user_seq = {"node_id":node_id, "seq":0, "timestamp":int(time.time()*1000)}
            self.user_seq_cache.set(node_id , user_seq)
            self.node_seq.insert_one(user_seq)
            return 0
        
