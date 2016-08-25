'''
Created on May 26, 2016

@author: abhinav
'''
import json





def test0():
    import cookies 
    import config
    
    auth_key = cookies.create_signed_value(config.SERVER_SECRET  , config.SERVER_AUTH_KEY_STRING ,json.dumps({'node_id':"asdasdasdasdas"}))
    print auth_key
    
    return True

def test_decode_key():
    import cookies
    import config
    print cookies.decode_signed_value(config.SERVER_SECRET, config.SERVER_AUTH_KEY_STRING, "cmVzdGF1cmFudDFfX3JlY2VwdGlvbg==|1471796646|69861d7be34901217fffff5db5ae33a61a4c95fc")




def test1():
    from websocket._core import create_connection
    from bson import json_util
    import cookies
    from database import Db
    import config
    import time
    
    
    db = Db()
    
    db.init()
    
    
    node_id = "DGQWQrnYng1464353050"# server
    
    # this is where user connects to nodes
    
    db.remove_client_nodes("abhinav")
    
    client_node_id1 = db.create_node("abhinav", None, None, None)# client
    client_node_id2 = db.create_node("abhinav", None, None , None)# client
    
    
    print "creating node" , node_id, client_node_id1, client_node_id2
    
    auth_key_client1 = cookies.create_signed_value(config.SERVER_SECRET , config.SERVER_AUTH_KEY_STRING, json_util.dumps({'node_id':client_node_id1}))
    
    
    
    auth_key_client2 = cookies.create_signed_value(config.SERVER_SECRET , config.SERVER_AUTH_KEY_STRING ,json_util.dumps({'node_id':client_node_id2}))
    
    
    ws1 = create_connection("ws://192.168.1.17:8081/connect?auth_key="+auth_key_client1)
    print "created_connection 1"
    
    time.sleep(1)
    
    ws2 = create_connection("ws://192.168.1.17:8081/connect?auth_key="+auth_key_client2)
    print "created_connection 2"
    
    
    ws1.send(json_util.dumps({'dest_id':client_node_id2, "payload":"Hello world" }))
    time.sleep(0.1)
    print ws2.recv()
    time.sleep(0.1)
    print ws1.recv()
    
    ws1.close()
    ws2.close()
    
def test2():
    from websocket._core import create_connection
    from bson import json_util
    import cookies
    from database import Db
    import config
    import time
    
    db = Db()    
    db.init()
    # this is where user connects to nodes    
    db.remove_client_nodes("abhinav")    
    client_node_id1 = db.create_node("abhinav", None, None, None)# client
    client_node_id2 = db.create_node("abhinav", None, None , None)# client
    
    print "creating nodes" , client_node_id1, client_node_id2
    
    auth_key_client1 = cookies.create_signed_value(config.SERVER_SECRET , "auth", json_util.dumps({'node_id':client_node_id1}))
    
    auth_key_client2 = cookies.create_signed_value(config.SERVER_SECRET , "auth",json_util.dumps({'node_id':client_node_id2}))
        
    ws1 = create_connection("ws://192.168.1.146:8081/connect?auth_key="+auth_key_client1)
    print "created_connection 1"
    
    time.sleep(1)
    
    ws2 = create_connection("ws://192.168.1.146:8083/connect?auth_key="+auth_key_client2)
    print "created_connection 2"
    
    
    ws1.send(json_util.dumps({'dest_id':client_node_id2, "payload":"Hello world" }))
    time.sleep(0.1)
    print ws2.recv()
    time.sleep(0.1)
    print ws1.recv()
    
    ws1.send_close()
    ws2.send_close()


def load_test_single_server(n=100, k_procs = 1, n_messages=100):
    #make n node_ids
    #register n_node_ids
    #parallel k_threads  each of which send a message from src to dest , with random message ids for one minute
    #stop sending after one minute and see if all messages recieved
    import gevent
    from gevent import monkey
    monkey.patch_all(thread=False)
    
    import cookies 
    import config
    import time
    from websocket._core import create_connection
    from bson import json_util
    import json
    from time import sleep
    import util_funcs
    import random
    import threading
    import urllib
    import os
    
    
    node_ids = json_util.loads(open("node_ids.txt","r").read())
    
    if(not node_ids or len(node_ids)<n):
        if(node_ids==None):
            node_ids = []
        while(len(node_ids)<n):
            node_ids.append(util_funcs.get_random_id(10))
        
        open("node_ids.txt","w").write(json_util.dumps(node_ids))
        
        
    node_connections = {}
    
    def send_message_into_network(src_id, dest_id):
        
            message = {"id":util_funcs.get_random_id(10), "src_id": src_id , "dest_id":dest_id, "payload":"same random payload"}
            node_connections[src_id].send(json_util.dumps(message))
            
    
    messages_recieved = []
    last_message_recieved = [-1]
    def recv_message(ws):

        try:
            while(True):
                message = ws.recv()
                messages_recieved.append(True)
                last_message_recieved[0] = time.time()
        except Exception as ex:
            pass
            
        
        
        
        
        
        
    tasklets = []
        
    #called from threads
    def open_connections(node_ids , i , j, id):
        print "Spinning a thread ", id, "node_ids from ", i , j
        for p in range(i , j):
            node_id = node_ids[p]
            try:
                auth_key = cookies.create_signed_value(config.SERVER_SECRET  , config.SERVER_AUTH_KEY_STRING ,json.dumps({'node_id':node_id}))
#               ws = create_connection("ws://104.199.129.250:8081/connectV2?auth_key="+auth_key)
                ws = create_connection("ws://192.168.1.146:8081/connectV2?auth_key="+auth_key)
                #print "created connection", ws
                node_connections[node_id] = ws
                tasklets.append(gevent.spawn(recv_message , ws))
            except Exception as e: 
                print "exception opening connection"
                pass
    
    def close_connections(node_ids , i , j, id):
        for p in range(i , j):
            node_id= node_ids[p]
            node_connections[node_id].close()
            
    process_count = 0
    start = time.time()
    while(process_count < k_procs):
        c = n/k_procs
        #each thread spins off new connections which inturn spawn gevent threads
        pid = os.fork()
        if(pid != 0):
            i  = c*process_count
            j =  c*(process_count+1)
            open_connections(node_ids, i , j, pid)
            
            # start sending messages onto network
            n= 0 
            
            while(n<n_messages):
                try:
                    n+=1
                    src_id = None
                    dest_id = None
                    
                    while(src_id==dest_id):
                        src_id = node_ids[random.randrange(i, j)]
                        dest_id = node_ids[random.randrange(i, j)]
                     
                    send_message_into_network(src_id, dest_id)
                except Exception as ex:
                    print "exception sending messages", ex
            print "sent ", n , " messages onto network process::",pid,  time.time()-start, "seconds"
            
            p = 0
            while(p<50 and len(messages_recieved)<n_messages):
                gevent.sleep(1)
                p+=1
            print "messages recieved :: pid:: " , pid , len(messages_recieved), "in", last_message_recieved[0] -start, " seconds "                
                
            print " closing connections "
            close_connections(node_ids, i , j , pid)
            
            gevent.joinall(tasklets)
            break
        else:
            process_count+=1
    
    '''
    
    #########mac
    sudo launchctl limit maxfiles 1000000 1000000
    
    $ sysctl kern.maxfiles
    kern.maxfiles: 12288
    $ sysctl kern.maxfilesperproc
    kern.maxfilesperproc: 10240
    $ sudo sysctl -w kern.maxfiles=1048600
    kern.maxfiles: 12288 -> 1048600
    $ sudo sysctl -w kern.maxfilesperproc=1048576
    kern.maxfilesperproc: 10240 -> 1048576
    
    $ sudo sysctl -w kern.ipc.somaxconn=2048
    
    
    $ ulimit -S -n
    256
    $ ulimit -S -n 1048576
    $ ulimit -S -n
    1048576
    
    '''
def mongo_db_test():
    from database_mongo import Db
    import time
    db = Db()
    db.init()
    current_timestamp = time.time()*1000
    db.add_pending_messages("asdasdasd", 1, "{}", current_timestamp)
    db.add_pending_messages("asdasdasd", 2, "{}")
    time.time()*1000
    
    print db.fetch_inbox_messages("asdasdasd", -1, -1, current_timestamp-100)
    
def mongo_db_test_1():
    # to test fetch node
    from database_mongo import Db
    import time
    db = Db()
    db.init(user_name="abhinav", password="$samosa$", host="104.199.129.250", namespace="samosa_messaging_v2")
    
#     print db.fetch_inbox_messages("df44c88cdaa36e4de1c5bf9c36968078", -1, -1, 1468524512476)
#
    db.update_node_info("g3cK4ljIyh1468240301", num_connections=1650, num_max_connections=1500)
    print db.get_a_connection_node()
    

if __name__ == "__main__":
    #load_test_single_server()
    #test2()
    #mongo_db_test()
    #mongo_db_test_1()
    test0()
    test_decode_key()
    #mongo_db_test_1()