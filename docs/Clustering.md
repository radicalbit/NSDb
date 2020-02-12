# Clustering

NSDb has been designed to be a cluster, which means that if a single instance is running, it is a cluster of one node. 
All primary shards reside on the single node. 
No replica shards can be allocated, therefore the cluster state remains yellow. The cluster is fully functional but is at risk of data loss in the event of a failure.
You add nodes to a cluster to increase its capacity and reliability. By default, a node is both a data node and eligible to be elected as the master node that controls the cluster. You can also configure a new node for a specific purpose, such as handling ingest requests. For more information, see Nodes.

As mentioned in the overall architectural documentation, since NSDb strongly rely on akka cluster,