package io.radicalbit.nsdb.split_brain

import akka.cluster.Cluster
import io.radicalbit.nsdb.split_brain.configs.SplitBrainThreeNodeSpecConfig

import scala.concurrent.duration._

class SplitBrainThreeNodesSpecMultiJvmNode1 extends SplitBrainThreeNodesSpec
class SplitBrainThreeNodesSpecMultiJvmNode2 extends SplitBrainThreeNodesSpec
class SplitBrainThreeNodesSpecMultiJvmNode3 extends SplitBrainThreeNodesSpec

/**
 * Test Class in which split brain is reproduced with a cluster of three nodes
 */
class SplitBrainThreeNodesSpec() extends MultiNodeBaseSpec(SplitBrainThreeNodeSpecConfig) {

  import SplitBrainThreeNodeSpecConfig._

  val side1 = Vector(node1, node2)
  val side2 = Vector(node3)

  "SplitBrainThreeNodesSpec" must {
    "start node-1" in within(30 seconds) {
      runOn(node1) {
        Cluster(system).join(addressOf(node1))
        awaitClusterNodesForUp(node1)
      }

      enterBarrier("node-1-up")
    }

    "start node-2" in within(30 seconds) {
      runOn(node2) {
        Cluster(system).join(addressOf(node1))
        awaitClusterNodesForUp(node1, node2)
      }
      enterBarrier("node-2-up")
    }

    "start node-3" in within(30 seconds) {
      runOn(node3) {
        Cluster(system).join(addressOf(node1))
        awaitClusterNodesForUp(node1, node2, node3)
      }
      enterBarrier("node-3-up")
    }

    "handle split brain scenario" in within(60 seconds) {
      runOn(node1) {
        for (role1 <- side1; role2 <- side2) switchOffConnection(role1, role2)
      }
      enterBarrier("links-failed")

      runOn(side1: _*) {
        awaitClusterLeader(side1: _*)
        awaitAssert(side2.foreach(role => cluster.down(addressOf(role)))) // manually healing the cluster
        awaitExistingMembers(side1:_*) // the new cluster is composed only by side1 nodes
      }

      runOn(side2:_*) {
        an[java.lang.AssertionError] shouldBe thrownBy(awaitSelfDowningNode(5 seconds)) // demonstrating that isolated node doesn't down by itself
      }
      enterBarrier("3 nodes split-brain")
    }
  }
}
