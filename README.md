Allspark Realtime server:-   Scalable multinode secure realtime messaging router. \
Can build  Chatting/ realtime messaging / gaming server. 

Features:
1) Simple and powerful
2) websocket based
3) software loadbalanced
4) heavily cached
5) Scalable.
6) Json based.
7) Pending messages and inbox. ( Definite delivery and syncing request).


Getting started: 

##Database:
1) Mongodb connector
2) Google datastore connector (buggy)

-Start mongodb instance and update "config.py" with DB_** variables for username, password , host

##start nodes as below

nohup python server.py --host_address=104.155.228.18 --port=8080 --proxy_80_port=message-server1-8080.appsandlabs.com --force=True --log_level=debug > nohup-8080.out 2>&1 &

-starts a new node with given ip address and port, (optionally mention) --proxy_80_port  , --force option ingnores the case when you stopped the old node and restarted again.

##to start more nodes:: 
nohup python server.py --host_address=104.155.228.18 --port=8081 --proxy_80_port=message-server1-8081.appsandlabs.com --force=True --log_level=debug > nohup-8081.out 2>&1 &

nohup python server.py --host_address=104.155.228.18 --port=8082 --proxy_80_port=message-server1-8082.appsandlabs.com --force=True --log_level=debug  > nohup-8082.out 2>&1 &


#client:
First you would need an authentication key to make any request, on your app you should create one after your user does login/signup, basically a SHA encrypted node_id.

After you have the auth_key. Do a http request with  "get_pre_connection_info=True&auth_key=USER_AUTH_KEY_HERE" to the server. In response you would get a validation token and node info that contains ip , port and other info to open a web socket to.

Open a websocket to the server including auth_key and validation_token in the websocket url.
You can now send json paylods of this format 

{	dest_id: DEST_NODE_ID, 
	dest_session_id: DEST_SESSION_ID,
	payload:  YOUR_PAYLOAD,
	payload1: YOUT_PAYLOAD_MORE, 
	payload2: YOUT_PAYLOAD_SO_MORE,
}
 
when reciving the message you would receive a json packet of the same format but includes a src_id too.
 
##sessions:
A client can create/join/unjoin session (even anonymouly) by issuing a create_session , join_session , unjoin_session http requests.


 
##Anonymous messaging:
Anonymous messaging is supported by creating a session and users joining the session anonymously.



Feeling interested ?

##Core concepts:
1. Node: Every server , device on the network  is a node , which must have an unique node id.
2. Connection:  represents from_node_id  , to_node_id , which is basically edge on the graph. Each of these edge has an id 'connection_id' and has a send queue , where the messages to be sent are put into(sending queues on both sides).
3. You can create connections to nodes if they have addresses.
4: Client: client can have multiple nodes. Just like you have a computer, mobile , etc .
5. Session: basically a group of nodes together are inside a session.
6. Software loadbalanced. 

##The Mechanics:
A graph of nodes/connections.

- There are nodes everywhere that are connected to each other. Each node must have an unique id. In real sense, every device of a user has a unique node_id, and so is every server.
- You can send messages to a single node or in general send to a session which inturn will forward to all nodes in that session. 
- In the connection graph , with unique node_ids , we register each edge with a unique connection_id.
(Between two nodes there could be multiple connections.)

Each sent message should have a dest_id , dest_client_id or dest_session_id set , this is mandatory to forward message. otherwise it is considerd as a config message/ping.



##Workflow:
1) The client makes a call to get preconnection info first which should return the node information to connect to including a connection validation key.

2) Connect to the node address directly and include validation_key in the request as get parameters.  After the  'node1' makes a connection to server 'node2', on the node2, we will register a connection in db  (node1 , node2 ,connection_id ) each connection has a unique id( like addressing an edge ). 

3) Each connection(between two nodes) on a server has a send queue, when you want to send , the message is put into the queue when you intent to send it.

4) When a connection breaks, we destory the corresponding databse entry. 

5) When the node you want to forward doesn't have a direct connection, we can either 
	a) open a connection to it directly if it has a direct address 
		(OR)
	b) We open a connection to the node that this node is connected to.
	    (or)
	c) The node is not connected.  we send push notifications incase of android/ios, (supports gcm key for android) and 			store the message in pending messages.
	
5) Done. 

##Proxy connections:
For users who cannot connect to custom ports which server start , we additonally provide a proxy address for a node. Supporting nginx sample config added to project.


##websockets backed by gevent.

##performance testing/benchmarking:
- Tested at 50,000 messages , 20 seconds , 1% errors on a single micro instance.



-Free to Contribute.

