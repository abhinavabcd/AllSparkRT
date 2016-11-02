'''
Created on May 23, 2016

@author: abhinav
'''


from pymongo import MongoClient
import util_funcs
from lru_cache import LRUCache
import gevent
from logger import logger
import time
import pymongo
import collections

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
    node_seq_cache = LRUCache(10000)
    session_node_ids_cache = LRUCache(100000)
    session_info_cache = LRUCache(10000)
    
    def init(self, db_name,  user_name="", password="", host="127.0.0.1", namespace=""):
        client = MongoClient("mongodb://"+((user_name+":"+password+"@") if user_name else "" )+host+"/"+namespace)
        self.db = client[db_name]
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
    def get_node_by_id(self, node_id, strict_check = True, force_refresh_from_db=False):
        node = self.node_cache.get(node_id)
        if(node and not force_refresh_from_db):
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
        node = self.get_node_by_id(node_id)
        u = {"$set":{}}
        
        _can_join = 0
        _max_connections = node["num_max_connections"] if node and node.get("num_max_connections",None) else 0
        _num_connections = node["num_connections"] if node and node.get("num_connections",None) else 0
        
        if(num_connections!=-1):
            u["$set"]["num_connections"] = _num_connections= num_connections
        if(num_max_connections!=-1):
            u["$set"]["num_max_connections"] = _max_connections = num_max_connections 
        
        _can_join = _max_connections - _num_connections
        u["$set"]["can_join"] = _can_join
        
        
        
            
        
        if(num_msg_transfered!=-1):
            if(not u.get("$inc",None)):
                u["$inc"] = {}
            u["$inc"]["num_msg_transfered"] =  num_msg_transfered
            u["$set"]["num_msg_transfer_rate"] =  num_msg_transfered       
        
        if(proxy_80_port!=None):
            u["$set"]["proxy_80_port"] =  proxy_80_port
        
            
        
        u["$set"]["last_stats_refresh_timestamp"] = int(time.time())
        self.nodes.update_one({"node_id":node_id} , u)
        #refetch node from db
    
        node = self.get_node_by_id(node_id, force_refresh_from_db=True)
    
    
    def disable_serving_node(self, node_id):
        node = self.get_node_by_id(node_id)
        u = {"$set":{}}
        u["$set"]["last_stats_refresh_timestamp"] = 0# implies disabled
        self.nodes.update_one({"node_id":node_id}, u)
        
        
                
    def get_a_connection_node(self, session_id=None, need_80_port=False):
        if(session_id!=None):
            session = self.get_session_by_id(session_id)
            fixed_node_id = session.get("is_fixed_node_id", None)
            if(fixed_node_id):
                return self.get_node_by_id(fixed_node_id)
                
        query = {"addr": {"$ne": None} , "last_stats_refresh_timestamp" :{"$gt" : int(time.time()) - 5*60 +1  }, "can_join": {"$gt": 0}}
        if(need_80_port):
            query["proxy_80_port"] = {"proxy_80_port": {"$ne":None}}
            
        nodes = self.nodes.find(query).sort([("_id",1)])
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
        
        node_ids = self.session_node_ids_cache.get(session_id)
        if(node_ids):
            return node_ids
        ret = collections.OrderedDict()
        session =self.get_session_by_id(session_id)
        max_users_to_notify = session.get("notify_only_last_few_users",256)

        session_nodes= self.session_nodes.find({"session_id":session_id}).sort([("_id",-1)])
        for i in session_nodes[:max_users_to_notify]:
            ret[i["node_id"]] = (i["node_id"], i.get("anonymous_node_id",None))
            
        self.session_node_ids_cache.set(session_id, ret)
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
    
    def create_session(self , node_id, session_id=None, session_type=0, session_game_master_node_id=None, notify_only_last_few_users=None, anyone_can_join=None):
        session_id = session_id or util_funcs.get_random_id(10)
        if(not notify_only_last_few_users):
            notify_only_last_few_users = -1
        notify_only_last_few_users = max(256 , int(notify_only_last_few_users))
        self.sessions.insert_one({"session_id":session_id, "node_id":node_id , "created_at":time.time(), "session_type":session_type, "session_game_master_node_id":session_game_master_node_id, "notify_only_last_few_users" :notify_only_last_few_users, anyone_can_join:anyone_can_join})
        return session_id
    
    def get_session_by_id(self, session_id):
        session = self.session_info_cache.get(session_id)
        if(not session):
            session = self.sessions.find_one({"session_id":session_id})
            self.session_info_cache.set(session_id , session)
        return session
    
    def join_session(self, session_id , node_id, is_anonymous=False, update_in_db=True, anonymous_node_id=None):
        
        
        node_ids = self.session_node_ids_cache.get(session_id)
        
        if(node_ids):
            node_info = node_ids.get(node_id, None)
            if(node_info):
                #already in session
                return node_info
                
        if(update_in_db):
            doc = {"session_id":session_id, "node_id":node_id}
            if(is_anonymous):
                anonymous_node_id = anonymous_node_id or "anonymous_"+util_funcs.get_random_id(10)
                doc["anonymous_node_id"] = anonymous_node_id
                
            result = self.session_nodes.insert_one(doc)
            if(not result.inserted_id):
                return None
        else:
            doc = {"session_id":session_id, "node_id":node_id}
            ret = self.session_nodes.find_one(doc)
            if(ret):
                anonymous_node_id = ret["anonymous_node_id"]
                
        #below code is to only only notify_only_last_few_users
        node_ids = self.session_node_ids_cache.get(session_id)
        session =self.get_session_by_id(session_id)
        notify_only_last_few_users = session.get("notify_only_last_few_users",-1)
        if(notify_only_last_few_users!=-1):# -1 means every one , possitve number mean , last n users will be notified
            if(len(node_ids)>notify_only_last_few_users and node_ids and not node_ids.get(node_id, None)):#remove the first 
                node_ids.popitem(last=False)

        if(node_ids):
            if(not node_ids.get(node_id, None)):
                node_ids[node_id] = (node_id, anonymous_node_id)
                    
        return (node_id, anonymous_node_id)
    
    
    def unjoin_session(self, session_id, node_id , update_in_db=True):
        if(update_in_db):
            result = self.session_nodes.delete({"session_id":session_id, "node_id":node_id})
        
        node_ids = self.session_node_ids_cache.get(session_id)
        if(node_ids):
            del node_ids[node_id]
        
        return
    
    
    def reveal_anonymity(self, session_id, node_id, update_in_db=True):
        if(update_in_db):
            result = self.session_nodes.update_one({"session_id":session_id, "node_id":node_id}, {"anonymous_node_id":None})
        
        node_ids = self.session_node_ids_cache.get(session_id)
        if(node_ids):
            node_ids[node_id] = (node_id, None)
        
        return
        
    
    def remove_client_nodes(self, client_id):
        result = self.nodes.delete_many({"client_id":client_id})
    
    
    
    def add_pending_messages(self, node_id, message_type, message_json, current_timestamp=None):
        
        seq = self.get_seq(node_id)
        if(not current_timestamp):
            current_timestamp = int(time.time()*1000)
            
        self.pending_messages.insert_one({"node_id_seq":node_id+"__"+str(seq), "message_type":message_type, "message_json":message_json, "timestamp":current_timestamp})
            
    def fetch_inbox_messages(self, node_id , from_seq=-1, to_seq = -1,  timea=None , timeb=None):   
        if(to_seq==-1):
            to_seq = self.get_seq(node_id, update=False)
        if(from_seq==-1):
            from_seq = max(0 , to_seq - 50)
            
        if(timeb==None):
            timeb=(time.time()*1000)
        if(timea==None):
            timea=0
        ret = []
        flag = False
        more = False
        for i in range(to_seq, from_seq-1, -1):
            pending_messages = self.pending_messages.find({"node_id_seq": node_id+"__"+str(i)})
            pending_messages = sorted(pending_messages, key = lambda x:x.get("timestamp", 0) , reverse=True)# read from end
            for j in pending_messages:
                timestamp = j.get("timestamp",0)
                is_invalid = timestamp<=timea or timestamp >= timeb
                if(is_invalid):
                    continue
                if(len(ret)>100):
                    flag  = True #no more than 100 messages
                    more = True
                    
                ret.append(j)
                
            if(flag):
                break
        return map(lambda x: x["message_json"] ,  ret), i, to_seq, more
                    
            


    def get_seq(self, node_id, update=True):
        
        node_seq = self.node_seq_cache.get(node_id)
        if(not node_seq):
            node_seq = self.node_seq.find_one({"node_id":node_id})
            if(node_seq!=None):
                self.node_seq_cache.set(node_id, node_seq )
            
        if(node_seq):#syn from db if time to check expired
            ret = node_seq["seq"]
            current_timestamp = time.time()*1000
            if(current_timestamp - node_seq["timestamp"] >30*60*1000):
                _node_seq_in_db = self.node_seq.find_one({"node_id":node_id})
                if(_node_seq_in_db["seq"]>ret):
                    node_seq  =  _node_seq_in_db
                    self.node_seq_cache.set(node_id, node_seq)
                    ret = node_seq["seq"]
                elif(update):
                    ret+=1
                    self.node_seq.update_one({"node_id":node_id}, {"$set": {"timestamp": current_timestamp, "seq":ret}})              
                    node_seq["timestamp"] = time.time()*1000
                    node_seq["seq"] = ret
            return ret
        else:
            node_seq = {"node_id":node_id, "seq":0, "timestamp":int(time.time()*1000)}
            self.node_seq_cache.set(node_id , node_seq)
            self.node_seq.insert_one(node_seq)
            return 0
