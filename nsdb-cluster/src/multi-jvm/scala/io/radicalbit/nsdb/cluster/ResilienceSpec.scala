package io.radicalbit.nsdb.cluster

import akka.cluster.MemberStatus
import akka.pattern.ask
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.TestProbe
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache.{GetNodesBlackListFromCache, NodesBlackListFromCacheGot}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.GetWriteLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{GetWriteLocationsFailed, WriteLocationsGot}
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetNodeChildActors, NodeChildActorsGot}
import io.radicalbit.nsdb.{NSDbMultiNodeDynamicNameActorsSupport, NSDbMultiNodeFailuresSupport, NSDbMultiNodeSpec}

import scala.concurrent.duration._

object ResilienceSpec extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")

  commonConfig(ConfigFactory.parseResources("application.conf"))
  testTransport(on = true)
}

class ResilienceSpecMultiJvmNode1 extends ResilienceSpec
class ResilienceSpecMultiJvmNode2 extends ResilienceSpec
class ResilienceSpecMultiJvmNode3 extends ResilienceSpec
class ResilienceSpecMultiJvmNode4 extends ResilienceSpec
class ResilienceSpecMultiJvmNode5 extends ResilienceSpec

abstract class ResilienceSpec extends MultiNodeSpec(ResilienceSpec) with NSDbMultiNodeSpec with NSDbMultiNodeDynamicNameActorsSupport with NSDbMultiNodeFailuresSupport {

  import ResilienceSpec._

  override def initialParticipants: Int = roles.size

  implicit val timeout: Timeout = Timeout(5 seconds)

  val probe = TestProbe()

  "NSDb Cluster" must {

    "join cluster" in {
      cluster.join(node(node1).address)

      awaitAssert {
        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe initialParticipants
        NSDbClusterSnapshot(system).nodes.size shouldBe initialParticipants
        nodeActorGuardian ! GetNodeChildActors
        expectMsgType[NodeChildActorsGot]
      }

      enterBarrier("joined")
    }

    "retrieve locations given a full replication factor" in {
      metadataCoordinator() ! GetWriteLocations("db", "namespace", "metric", 1L, initialParticipants)

      val writeLocationsGot = awaitAssert {
        expectMsgType[WriteLocationsGot]
      }

      writeLocationsGot.locations.size shouldBe 5
      writeLocationsGot.locations.map(_.node.nodeFsId).sorted shouldBe Seq(node1.name, node2.name, node3.name, node4.name, node5.name)

    }

    "create a blacklist for unreachable nodes" in {
      runOn(node1) {
        switchOffConnection(node1, node2)
      }

      enterBarrier("after-node1-node2-failure")

      runOn(node3, node4, node5) {
        awaitAssert {
          NSDbClusterSnapshot(system).nodes.size shouldBe initialParticipants - 2
        }
        awaitAssert {
          NSDbClusterSnapshot(system).nodes.map(_.nodeFsId).toSet shouldBe Set(node3.name, node4.name, node5.name)
        }
        awaitAssert{
          val blackList = (metadataCache() ? GetNodesBlackListFromCache).mapTo[NodesBlackListFromCacheGot].await.blacklist
          blackList.size shouldBe 2
          blackList.map(_.nodeFsId) shouldBe Set(node1.name, node2.name)
        }
      }

      enterBarrier("1 nodes cluster")
    }

    "refuse to get locations for a replication factor higher then the alive members" in {
      metadataCoordinator() ! GetWriteLocations("db", "namespace", "metric", 1L, initialParticipants)

      expectMsgType[GetWriteLocationsFailed]

      runOn(node3, node4, node5) {
        metadataCoordinator() ! GetWriteLocations("db", "namespace", "metric", 1L, initialParticipants -2)

        val writeLocationsGot = awaitAssert {
          expectMsgType[WriteLocationsGot]
        }
        
        writeLocationsGot.locations.size shouldBe 3
        writeLocationsGot.locations.map(_.node.nodeFsId).sorted shouldBe Seq(node3.name, node4.name, node5.name)
      }
    }

    "restore cluster state failure resolution" in {
      runOn(node1) {
        switchOnConnection(node1, node2)
      }

      enterBarrier("node1-node2-failure-repaired")

      expectNoMessage(10 seconds)
      awaitAssert {
        NSDbClusterSnapshot(system).nodes.size shouldBe initialParticipants
      }
    }
  }

}
