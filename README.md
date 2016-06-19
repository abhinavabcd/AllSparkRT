There are nodes everywhere that are connected to each other. Each node must have an unique id. Client has multiple nodes.
You can send messages to a single node or in general send to a client which inturn will send to all the connected client ndoes.

In the connection graph , with unique node_ids , we register each edge with a unique connection_id.
Between two nodes there could be multiple connections. 

Each send message should have a dest_id , dest_client_id or dest_session_id set , this is mandatory to forward message. 

1) The client makes a call to get preconnection info first which should return the node information to connect to.
2) After the client node1 makes a connection to server node2, on the node2, we will register a connection in db  (node1 , node2 ,connection_id ) each connection has a unique id( like addressing an edge ).
3) Each connection(between two nodes) on a server has a send queue, when you want to send , the message is put into the queue.
