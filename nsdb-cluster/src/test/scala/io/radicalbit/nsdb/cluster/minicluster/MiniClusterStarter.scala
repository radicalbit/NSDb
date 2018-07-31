package io.radicalbit.nsdb.cluster.minicluster

object MiniClusterStarter extends App with NsdbMiniCluster {
  override protected[this] def nodesNumber = 2
}
