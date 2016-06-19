'''
Created on May 26, 2016

@author: abhinav
'''

import cookies 
import config
from database import Db
from websocket._core import create_connection
from bson import json_util
import json
from time import sleep




def test0():
    print cookies.create_signed_value(config.SERVER_KEY , "auth","{'node':'node1'}")    
    print cookies.create_signed_value(config.SERVER_KEY , "auth","{'node' : 'node2'}")
    
    return True






def test1():
    db = Db()
    
    db.init()
    
    
    node_id = "DGQWQrnYng1464353050"# server
    
    # this is where user connects to nodes
    
    db.remove_client_nodes("abhinav")
    
    client_node_id1 = db.create_node("abhinav", None, None, None)# client
    client_node_id2 = db.create_node("abhinav", None, None , None)# client
    
    
    print "creating node" , node_id, client_node_id1, client_node_id2
    
    auth_key_client1 = cookies.create_signed_value(config.SERVER_KEY , config.SERVER_AUTH_KEY_STRING, json.dumps({'node_id':client_node_id1}))
    
    
    
    auth_key_client2 = cookies.create_signed_value(config.SERVER_KEY , config.SERVER_AUTH_KEY_STRING ,json.dumps({'node_id':client_node_id2}))
    
    
    ws1 = create_connection("ws://192.168.1.17:8081/connect?auth_key="+auth_key_client1)
    print "created_connection 1"
    
    sleep(1)
    
    ws2 = create_connection("ws://192.168.1.17:8081/connect?auth_key="+auth_key_client2)
    print "created_connection 2"
    
    
    ws1.send(json_util.dumps({'dest_id':client_node_id2, "payload":"Hello world" }))
    sleep(0.1)
    print ws2.recv()
    sleep(0.1)
    print ws1.recv()
    
    ws1.close()
    ws2.close()
    
def test2():
    db = Db()
    
    db.init()
    
    
    
    # this is where user connects to nodes
    
    db.remove_client_nodes("abhinav")
    
    client_node_id1 = db.create_node("abhinav", None, None, None)# client
    client_node_id2 = db.create_node("abhinav", None, None , None)# client
    
    
    print "creating nodes" , client_node_id1, client_node_id2
    
    auth_key_client1 = cookies.create_signed_value(config.SERVER_KEY , "auth", json.dumps({'node_id':client_node_id1}))
    
    
    
    auth_key_client2 = cookies.create_signed_value(config.SERVER_KEY , "auth",json.dumps({'node_id':client_node_id2}))
    
    
    ws1 = create_connection("ws://192.168.1.146:8081/connect?auth_key="+auth_key_client1) 
    print "created_connection 1"
    
    sleep(1)
    
    ws2 = create_connection("ws://192.168.1.146:8083/connect?auth_key="+auth_key_client2)
    print "created_connection 2"
    
    
    ws1.send(json_util.dumps({'dest_id':client_node_id2, "payload":"Hello world" }))
    sleep(0.1)
    print ws2.recv()
    sleep(0.1)
    print ws1.recv()
    
    ws1.send_close()
    ws2.send_close()
        
if __name__ == "__main__":
    test2()
    