# Clustering

## Architecture

NSDb has been designed to be a cluster, which means that even if only a single instance is running, it is a cluster of one node.  
Using only one node, no replicas are allocated, therefore fault tolerance and high availability are not enabled. 
Adding nodes to the cluster has the purpose to enable those two features increasing its capacity and reliability.

NSDb cluster is a Multi-master replication distributed system, that is an application which allows data to be stored in a set of members, 
and updated by any of them. 
All members expose all the interfaces (http, grpc) that can be use by clients to insert or query data. 
It is responsible to propagate the data modifications made by each member throughout all the other nodes 
and resolving any conflicts that might rise between concurrent changes made by different members. 
As mentioned in the overall architectural documentation, NSDb strongly relies on akka, in particular, regarding clustering and replication, on Akka Cluster. 

> Akka Cluster provides a fault-tolerant decentralized peer-to-peer based Cluster Membership Service with no single point of failure or single point of bottleneck. 
It does this using gossip protocols and an automatic failure detector.   


Before going on on the topic, it's mandatory to introduce three key concepts:
- **Shard**: a folder in a cluster node's file system that contains all the Lucene indexes necessary to execute all the supported kind of queries
- **Metadata**: all the information used to connote a metric and a query response (e.g. schemas, shards locations)
- **Location**: a specific metadata that defines a shard and that contains the timestamp boundaries and the node identifier

Different paradigms have been applied to manage metadata and shard data replication.

All metadata entries have been implemented as CRDTs (conflict-free replicated data types),
that are data structure that must include a monotonic conflicts merge function, which enables updates from any node without coordination. 
<br>Fortunately, **Akka Distributed Data** provides a predefined useful CRDTs basic 
data types (counters, sets, maps etc), hence all NSDb metadata entries are built on top of them. 

Replicas, on the other hand, are managed using an active-active replication strategy.
<br>This pattern consists in keeping multiple copies of a shard (replica) in different locations (thus in different nodes) and perform all modifications against all of them. 
In order to achieve this a cluster membership service (akka cluster) that allows discovery  of all replica locations is mandatory.
<br>In this scenario, we need entities, called **Coordinators** responsible to create new replicas and to manage their lifecycle,
in particular to gather all the write or read requests from the clients, propagate them to all the replica involved, and to monitor
all of them in order to ensure synchronization and to react to failures.
<br>Akka provides an infrastructure that enables all the feature described above. Basically, coordinators are modeled as parent actors while replicas as children actor watched (monitored) by the parents.
Every cluster node has got a Coordinator for write and for read operations.
                                                                                                          
## General Configuration

NSDb cluster configurations are designed to give the user the maximum flexibility. 
As mentioned in the previous section, replication is used to guarantee high availability and fault tolerance.
The number of replicas (stored in different nodes) that the system must manage is called **replication factor**. 

```$
nsdb.cluster {
    replication-factor = 2
    consistency-level = 1
}
```

Write process is eventually consistent. 
Briefly, there is an acknowledge process that is performed synchronously at every write request; if it went without errors, a positive response is returned to the client. 
Users can configure a finer grained tuning by specifying the number of replicas that must be acknowledged before returning a write response, i.e. **consistency level**.
The remaining replicas, the difference between replication factor and consistency level will be written asynchronously.
 

## Metric System

NSDb implements a basic metric system, that collects system data (e.g. cpu usage, memory and disk occupation) and use it for selecting nodes 
with the highest capacity to store replicas.
This implementation has been largely inspired by the akka adaptive routing feature.
It is possible to choose different capacity calculation criteria like in the following snippet:

```
nsdb.cluster {
    # metrics-selector = heap
    # metrics-selector = load
    # metrics-selector = cpu
    # metrics-selector = mix
    metrics-selector = disk
}
```

## Cluster Modes

NSDb supports 3 cluster modes. All of them leverages the powerful akka discovery extension.
To select one mode, it must be set the following config parameter:

```
nsdb.cluster {
    #mode = native
    #mode = k8s-api
    #mode = k8s-dns
}
```

#### Native

This mode allows to install an NSDb cluster on a set of on-premise machines that must be known at bootstrap time.
Therefore, in addition to the general cluster configuration, the list of nodes must be provided as in the following example:

```
nsdb.cluster {
    required-contact-point-nr = 1
    endpoints = [
        {
            host = {host1}
            port = 8558
        },
        {
            host = {host2}
            port = 8558
        },
        {
            host = {host3}
            port = 8558
        }]
}
```  

It is possible to notice the presence of the additional parameter __required-contact-point-nr__ 
which indicates the minimum number of nodes that must be participating to the membership service in order to create the cluster.
This parameter can be very useful in case of failures in the bootstrap process or during the cluster lifecycle.
Consider a cluster of 10 nodes, correctly bootstrapped, with required contact point set to 5.
If less than 5 nodes were failing, they are marked as unreachable but the cluster continues to be operative, 
but if more than 5 nodes are unreachable, the entire cluster will be shut-down.

#### k8s-api

This mode uses the kubernetes Java api in order to lookup for NSDb pods that are detected and joined to the cluster.
<br>It is the most simple way to deploy a cluster, since no additional configurations are required, but, on the other hand, the only kubernetes entity supported is the deployment. 
It's not possible to create a kubernetes statefulset using this mode.
Below an example of 

[k8s api example](../k8s/nsdb-cluster-deployment.yml)

#### k8s-dns

This mode leverages DNS lookup queries in order to detect pods and bootstrap the cluster.
<br>It is more flexible that the previous one because it's possible to install a k8s deployment and a statefulset as well.
There is an additional entity, a kubernetes headless service, that must be created so that readiness probes don’t interfere with bootstrap.

[k8s dns example](../k8s/nsdb-cluster-stateful.yml)