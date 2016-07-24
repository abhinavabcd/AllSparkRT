Allspark Messaging server:-   Scalable multinode realtime messaging router in and under 1000 lines of code, where every message has a src and dest id's set and this server reads messages and routes them appropriately to relevant connections. 


#Core concepts:
1.  Node: Every server , device on the network  is a node , which must have an unique node id.
2.  Connection:  represents from_node_id  , to_node_id , which is basically edge on the graph. Each of these edge/connections is addressable by connection_id and has a send queue , where the messages to be sent are put into.
3.  You can create connections to nodes if they have addresses.
4:  Client: client can have multiple nodes. Just like you have a computer, mobile , etc .
5.  Session: basically a group of nodes together are inside a session.

#The story:
We have a graph of nodes/connections.

- There are nodes everywhere that are connected to each other. Each node must have an unique id. Client has multiple nodes.
- You can send messages to a single node or in general send to a client which inturn will send to all the connected client nodes. Or send it to a session , that will inturn send it to all nodes in that session.

In the connection graph , with unique node_ids , we register each edge with a unique connection_id.
Between two nodes there could be multiple connections.

Each send message should have a dest_id , dest_client_id or dest_session_id set , this is mandatory to forward message. otherwise it is considerd as a config message/ping.


#Process:
1) The client makes a call to get preconnection info first which should return the node information to connect to.
2) After the client 'node1' makes a connection to server 'node2', on the node2, we will register a connection in db  (node1 , node2 ,connection_id ) each connection has a unique id( like addressing an edge ).
3) Each connection(between two nodes) on a server has a send queue, when you want to send , the message is put into the queue.
4) When the node you want to forward doesn't have a direct connection, we can either 
	a) open a connection to it directly if possible else
	b) We open a connection to the node that the destination code is connected to.
5) Done.  Happy servering.






#Added google datastore connector and a generic mongodb connector.


#websockets backed by gevent.

#performance testing/benchmarking not yet done, contributors appreciated.
