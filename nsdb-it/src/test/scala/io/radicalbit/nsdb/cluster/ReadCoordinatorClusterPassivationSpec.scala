package io.radicalbit.nsdb.cluster

import io.radicalbit.nsdb.minicluster.{MiniClusterStarter, NsdbMiniCluster}

class ReadCoordinatorClusterPassivationSpec extends ReadCoordinatorClusterSpec {

  override val passivateAfter = java.time.Duration.ofSeconds(10)

  override lazy val minicluster: NsdbMiniCluster = new MiniClusterStarter(nodesNumber, passivateAfter)

  override def beforeAll(): Unit = {
    super.beforeAll()
    waitPassivation()
    waitPassivation()
  }
}
