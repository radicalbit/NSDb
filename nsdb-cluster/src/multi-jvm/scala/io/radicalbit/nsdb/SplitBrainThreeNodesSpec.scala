package io.radicalbit.nsdb

import akka.cluster.Cluster

import scala.concurrent.duration._

object SplitBrainThreeNodeSpecConfig extends SplitBrainSpecConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")

}

class SplitBrainThreeNodesSpecMultiJvmNode1 extends SplitBrainThreeNodesSpec
class SplitBrainThreeNodesSpecMultiJvmNode2 extends SplitBrainThreeNodesSpec
class SplitBrainThreeNodesSpecMultiJvmNode3 extends SplitBrainThreeNodesSpec

class SplitBrainThreeNodesSpec extends SplitBrainSpec(SplitBrainThreeNodeSpecConfig) {

  import SplitBrainThreeNodeSpecConfig._

  val side1 = Vector(node1, node2)
  val side2 = Vector(node3)

  "SplitBrainThreeNodesSpec" must {
    "start node-1" in within(30 seconds) {
      runOn(node1) {
        Cluster(system).join(addressOf(node1))
        waitForUp(node1)
      }

      enterBarrier("node-1-up")
    }

    "start node-2" in within(30 seconds) {
      runOn(node2) {
        Cluster(system).join(addressOf(node1))
        waitForUp(node1, node2)
      }
      enterBarrier("node-2-up")
    }

    "start node-3" in within(30 seconds) {
      runOn(node3) {
        Cluster(system).join(addressOf(node1))
        waitForUp(node1, node2, node3)
      }
      enterBarrier("node-3-up")
    }

    "handle split brain scenario" in within(60 seconds) {
      runOn(node1) {
        for (role1 <- side1; role2 <- side2) switchOffConnection(role1, role2)
      }
      enterBarrier("links-failed")

      runOn(side1: _*) {
        assertLeader(side1: _*)
        assertUnreachable(side2: _*)
      }

      runOn(side2: _*) {
        assertLeader(side2: _*)
        assertUnreachable(side1: _*)
      }
      enterBarrier("3 nodes split-brain")
    }
  }
}
