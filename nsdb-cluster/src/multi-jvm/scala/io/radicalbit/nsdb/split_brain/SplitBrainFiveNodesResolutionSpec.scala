package io.radicalbit.nsdb.split_brain

import akka.cluster.Cluster
import io.radicalbit.nsdb.split_brain.configs.SplitBrainFiveNodesResolutionSpecConfig

import scala.concurrent.duration._

class SplitBrainFiveNodesResolutionSpecMultiJvmNode1 extends SplitBrainFiveNodesResolutionSpec
class SplitBrainFiveNodesResolutionSpecMultiJvmNode2 extends SplitBrainFiveNodesResolutionSpec
class SplitBrainFiveNodesResolutionSpecMultiJvmNode3 extends SplitBrainFiveNodesResolutionSpec
class SplitBrainFiveNodesResolutionSpecMultiJvmNode4 extends SplitBrainFiveNodesResolutionSpec
class SplitBrainFiveNodesResolutionSpecMultiJvmNode5 extends SplitBrainFiveNodesResolutionSpec

class SplitBrainFiveNodesResolutionSpec extends SplitBrainSpec(SplitBrainFiveNodesResolutionSpecConfig) {

  import SplitBrainFiveNodesResolutionSpecConfig._

  val side1 = Vector(node1, node2)
  val side2 = Vector(node3, node4, node5)

  "SplitBrainFiveNodesResolutionSpec" must {
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

    "start node-4" in within(30 seconds) {
      runOn(node4) {
        Cluster(system).join(addressOf(node1))
        waitForUp(node1, node2, node3, node4)
      }
      enterBarrier("node-4-up")
    }

    "start node-5" in within(30 seconds) {
      runOn(node5) {
        Cluster(system).join(addressOf(node1))
        waitForUp(node1, node2, node3, node4, node5)
      }
      enterBarrier("node-5-up")
    }

    "solve split brain scenario" in within(60 seconds) {
      runOn(node1) {
        for (role1 <- side1; role2 <- side2) switchOffConnection(role1, role2)
      }
      enterBarrier("links-failed")

      runOn(side2: _*) {
        awaitForSurvivors(side2: _*)
        awaitForAllLeaving(side1: _*)
        awaitLeader(side2: _*)
      }

      runOn(side1: _*) {
        awaitSelfDowning()
      }

      enterBarrier("5 nodes split-brain solved")
    }
  }
}
