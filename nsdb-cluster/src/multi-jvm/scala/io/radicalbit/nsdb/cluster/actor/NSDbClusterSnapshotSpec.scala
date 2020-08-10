package io.radicalbit.nsdb.cluster.actor

import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import io.radicalbit.rtsae.STMultiNodeSpec

class NSDbClusterSnapshotSpecMultiJvmNode1 extends NSDbClusterSnapshotSpec
class NSDbClusterSnapshotSpecMultiJvmNode2 extends NSDbClusterSnapshotSpec
class NSDbClusterSnapshotSpecMultiJvmNode3 extends NSDbClusterSnapshotSpec

object NSDbClusterSnapshotSpecConfig extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")

  commonConfig(ConfigFactory.parseString("""
                                           |akka.loglevel = ERROR
                                           |akka.actor.provider = "cluster"
                                           |nsdb {
                                           | retry-policy {
                                           |    delay = 1 second
                                           |    n-retries = 2
                                           |  }
                                           |}
                                           |""".stripMargin))

}

class NSDbClusterSnapshotSpec extends MultiNodeSpec(NSDbClusterSnapshotSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import NSDbClusterSnapshotSpecConfig._

  def initialParticipants: Int = roles.size



}
