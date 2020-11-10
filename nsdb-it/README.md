# NSDb minicluster

NSDb minicluster library allows users to create a local cluster.
An application must mix-in the trait `NsdbMiniCluster`

Also the akka discovery endpoints must be configured in file `minicluster.conf` according to the number of nodes. 
E.g. for three nodes

```
discovery {
    config.services = {
      NSDb = {
        endpoints = [
          {
            host = "127.0.0.1"
            port = 8558
          },
          {
            host = "127.0.0.2"
            port = 8558
          },
          {
            host = "127.0.0.3"
            port = 8558
          }
        ]
      }
    }
  }
```  

```scala
import io.radicalbit.nsdb.minicluster.NSDbMiniCluster

object MiniClusterStarter extends App with NSDbMiniCluster {
  override protected[this] def nodesNumber              = 3
  override protected[this] def passivateAfter: Duration = Duration.ofHours(1)

  start()
}
}
```

Since minicluster relies on local loopback addresses, it might be necessary, on Mac, to set those up, 
while on linux it should not be needed
```
sudo ifconfig lo0 alias 127.0.0.2 up
sudo ifconfig lo0 alias 127.0.0.3 up
sudo ifconfig lo0 alias 127.0.0.4 up
```
