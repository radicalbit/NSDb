package io.radicalbit.nsdb.split_brain

import akka.cluster.Cluster
import io.radicalbit.nsdb.split_brain.configs.SplitBrainThreeNodesResolutionSpecConfig

import scala.concurrent.duration._

class SplitBrainThreeNodesResolutionSpecMultiJvmNode1 extends SplitBrainThreeNodesResolutionSpec
class SplitBrainThreeNodesResolutionSpecMultiJvmNode2 extends SplitBrainThreeNodesResolutionSpec
class SplitBrainThreeNodesResolutionSpecMultiJvmNode3 extends SplitBrainThreeNodesResolutionSpec

/**
 * Test Class in which split brain is reproduced and solved with a cluster of three nodes
 */
class SplitBrainThreeNodesResolutionSpec extends MultiNodeBaseSpec(SplitBrainThreeNodesResolutionSpecConfig) {

  import SplitBrainThreeNodesResolutionSpecConfig._

  val side1 = Vector(node1, node2)
  val side2 = Vector(node3)

  "SplitBrainThreeNodesResolutionSpec" must {
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

    "solve split brain scenario" in within(60 seconds) {
      runOn(node1) {
        for (role1 <- side1; role2 <- side2) switchOffConnection(role1, role2)
      }
      enterBarrier("links-failed")

      runOn(side1: _*) {
        awaitSurvivorsNodes(side1: _*)
        awaitAllLeavingNodes(side2: _*)
        awaitClusterLeader(side1: _*)
      }

      runOn(side2: _*) {
        awaitSelfDowningNode()
      }
      enterBarrier("3 nodes split-brain solved")
    }
  }
}
