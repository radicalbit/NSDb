package io.radicalbit.nsdb.split_brain

import akka.actor.{PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import io.radicalbit.nsdb.split_brain.DatabaseActorsGuardianForTest.WhoAreYou
import io.radicalbit.nsdb.split_brain.configs.ClusterSingletonWithSplitBrainResolutionSpecConfig

import scala.concurrent.duration._

class ClusterSingletonWithSplitBrainResolutionSpecMultiJvmNode1 extends ClusterSingletonWithSplitBrainResolutionSpec
class ClusterSingletonWithSplitBrainResolutionSpecMultiJvmNode2 extends ClusterSingletonWithSplitBrainResolutionSpec
class ClusterSingletonWithSplitBrainResolutionSpecMultiJvmNode3 extends ClusterSingletonWithSplitBrainResolutionSpec
class ClusterSingletonWithSplitBrainResolutionSpecMultiJvmNode4 extends ClusterSingletonWithSplitBrainResolutionSpec
class ClusterSingletonWithSplitBrainResolutionSpecMultiJvmNode5 extends ClusterSingletonWithSplitBrainResolutionSpec

/**
 * Split Brain resolved scenario with Cluster Singleton
 */
abstract class ClusterSingletonWithSplitBrainResolutionSpec extends MultiNodeBaseSpec(ClusterSingletonWithSplitBrainResolutionSpecConfig) {

  import ClusterSingletonWithSplitBrainResolutionSpecConfig._

  system.actorOf(
    ClusterSingletonManager.props(singletonProps = Props(classOf[DatabaseActorsGuardianForTest]),
      terminationMessage = PoisonPill,
      settings = ClusterSingletonManagerSettings(system)),
    name = "databaseActorGuardian"
  )

  val side1 = Vector(node1, node2)
  val side2 = Vector(node3, node4, node5)

  private def awaitWhoAreYou: Unit = awaitCond {
    val dbActorGuardian =
      system.actorOf(
        ClusterSingletonProxy.props(singletonManagerPath = "/user/databaseActorGuardian",
          settings = ClusterSingletonProxySettings(system)), name = "databaseActorGuardianProxy"
      )
    dbActorGuardian ! WhoAreYou
    expectMsgType[String] === "akka://MultiNodeBaseSpec/user/databaseActorGuardian/singleton"
  }

  "MultiJvmTestSpec" must {
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

    "start node-4" in within(30 seconds) {
      runOn(node4) {
        Cluster(system).join(addressOf(node1))
        awaitClusterNodesForUp(node1, node2, node3, node4)
      }
      enterBarrier("node-4-up")
    }

    "start node-5" in within(30 seconds) {
      runOn(node5) {
        Cluster(system).join(addressOf(node1))
        awaitClusterNodesForUp(node1, node2, node3, node4, node5)
      }
      enterBarrier("node-5-up")
    }

    "demonstrate that after cluster partition two cluster singletons exists" in within(2 minutes) {
      runOn(node1) {
        for (role1 <- side1; role2 <- side2) switchOffConnection(role1, role2)
      }
      enterBarrier("split-created")

      runOn(side2:_*) {
        awaitSurvivorsNodes(side2:_*)
        awaitAllLeavingNodes(side1: _*)
      }

      runOn(side1: _*) {
        awaitSelfDowningNode()
      }
      enterBarrier("singleton-msg-side2-cluster")

      runOn(side2:_*) {
        awaitWhoAreYou
      }
      enterBarrier("end-test")

    }
  }
}