package io.radicalbit.nsdb.split_brain

import akka.cluster.Cluster
import io.radicalbit.nsdb.split_brain.configs.SplitBrainFiveNodesSpecConfig

import scala.concurrent.duration._

class SplitBrainFiveNodesSpecMultiJvmNode1 extends SplitBrainFiveNodesSpec
class SplitBrainFiveNodesSpecMultiJvmNode2 extends SplitBrainFiveNodesSpec
class SplitBrainFiveNodesSpecMultiJvmNode3 extends SplitBrainFiveNodesSpec
class SplitBrainFiveNodesSpecMultiJvmNode4 extends SplitBrainFiveNodesSpec
class SplitBrainFiveNodesSpecMultiJvmNode5 extends SplitBrainFiveNodesSpec

class SplitBrainFiveNodesSpec extends MultiNodeSpecBase(SplitBrainFiveNodesSpecConfig) {

  import SplitBrainFiveNodesSpecConfig._

  val side1 = Vector(node1, node2)
  val side2 = Vector(node3, node4, node5)

  "SplitBrainFiveNodesSpec" must {
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

    "handle split brain scenario" in within(60 seconds) {
      runOn(node1) {
        for (role1 <- side1; role2 <- side2) switchOffConnection(role1, role2)
      }
      enterBarrier("links-failed")

      runOn(side2: _*) {
        awaitLeader(side2: _*)
        awaitAssert(side1.foreach(role => cluster.down(addressOf(role)))) // manually healing the cluster
        awaitExistingMembers(side2:_*) // the new cluster is composed only by side1 nodes
      }

      runOn(side1:_*) {
        an[java.lang.AssertionError] shouldBe thrownBy(awaitSelfDowning(5 seconds)) // demonstrating that isolated node doesn't down by itself
      }
      enterBarrier("5 nodes split-brain")
    }
  }
}
