package io.radicalbit.nsdb.cluster.actor

import akka.actor.ActorRef
import akka.cluster.{Member, MemberStatus}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.STMultiNodeSpec
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.NSDbNode
import io.radicalbit.nsdb.model.{Location, LocationWithCoordinates}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetNodeChildActors, NodeChildActorsGot}

import scala.concurrent.Await
import scala.concurrent.duration._

object MetadataSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseResources("application.conf"))

  nodeConfig(node1)(ConfigFactory.parseString("""
      |akka.remote.artery.canonical.port = 25520
    """.stripMargin))

  nodeConfig(node2)(ConfigFactory.parseString("""
      |akka.remote.artery.canonical.port = 25530
    """.stripMargin))

}

class MetadataSpecMultiJvmNode1 extends MetadataSpec {}

class MetadataSpecMultiJvmNode2 extends MetadataSpec {}

class MetadataSpec extends MultiNodeSpec(MetadataSpec) with STMultiNodeSpec with ImplicitSender {

  import MetadataSpec._

  override def initialParticipants = roles.size

  implicit val timeout: Timeout = Timeout(5.seconds)

  val selfMember: Member = cluster.selfMember
  val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
  val nodeActorGuardian: ActorRef = system.actorOf(NodeActorGuardianForTest.props(nodeName), name = s"guardian_${nodeName}_$nodeName")

  val nsdbNode1 = NSDbNode("localhost_2552", "node1", "volatile1")
  val nsdbNode2 = NSDbNode("localhost_2553", "node2", "volatile2")

  private def metadataCoordinatorPath(nodeName: String) = s"user/guardian_${nodeName}_$nodeName/metadata-coordinator_${nodeName}_${nodeName}_$nodeName"
  private def metadataCache(nodeName: String) = s"user/guardian_${nodeName}_$nodeName/metadata-cache_${nodeName}_${nodeName}_$nodeName"

  "Metadata system" must {

    "join cluster" in {
      cluster.join(node(node1).address)

      awaitAssert {
        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe 2
        NSDbClusterSnapshot(system).nodes.size shouldBe 2
        nodeActorGuardian ! GetNodeChildActors
        expectMsgType[NodeChildActorsGot]
      }

      enterBarrier("joined")
    }

    "add location from different nodes" in {

      runOn(node1) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = Await.result(
          system.actorSelection(metadataCoordinatorPath(nodeName)).resolveOne(5.seconds),
          5.seconds)

        awaitAssert {
          metadataCoordinator ! AddLocations("db", "namespace", Seq(Location("metric", nsdbNode1, 0, 1)))
          expectMsg(LocationsAdded("db", "namespace", Seq(Location("metric", nsdbNode1, 0, 1))))
        }
      }

      enterBarrier("after-add-locations")
    }

    "add metric info from different nodes" in {

      val metricInfo = MetricInfo("db", "namespace", "metric", 100, 30)

      runOn(node1) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = system.actorSelection(metadataCoordinatorPath(nodeName))

        awaitAssert {
          metadataCoordinator ! PutMetricInfo(metricInfo)
          expectMsg(MetricInfoPut(metricInfo))
        }
      }

      enterBarrier("after-add-metrics-info")
    }

    "get write locations" in {

      val selfMember = cluster.selfMember
      val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

      val metadataCoordinator = system.actorSelection(metadataCoordinatorPath(nodeName))

      metadataCoordinator ! GetWriteLocations("db", "namespace", "metric", 0)

      awaitAssert{
        expectMsgType[GetWriteLocationsBeyondRetention]
      }

      val currentTime = System.currentTimeMillis()

      metadataCoordinator ! GetWriteLocations("db", "namespace", "metric", currentTime)

      awaitAssert{
        val reply = expectMsgType[WriteLocationsGot]
        reply.db shouldBe "db"
        reply.namespace shouldBe "namespace"
        reply.metric shouldBe "metric"
        reply.locations.size shouldBe 2
      }

      enterBarrier("after-get-write-locations")
    }

    "manage outdated locations" in {
      val selfMember = cluster.selfMember
      val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
      val metadataCoordinator = system.actorSelection(metadataCoordinatorPath(nodeName))

      metadataCoordinator ! GetOutdatedLocations

      awaitAssert{
        expectMsgType[OutdatedLocationsGot].locations.size shouldBe 0
      }

      enterBarrier("no-outdated-locations")

      val outdatedLocations = Seq(
        LocationWithCoordinates("db", "namespace", Location("metric", nsdbNode1, 0,1)),
        LocationWithCoordinates("db1", "namespace", Location("metric1", nsdbNode2, 1,4)),
        LocationWithCoordinates("db", "namespace1", Location("metric", nsdbNode1, 0,1)),
        LocationWithCoordinates("db", "namespaces", Location("metric", nsdbNode2, 0,1))
      )

      metadataCoordinator ! AddOutdatedLocations(outdatedLocations)
      awaitAssert{
        expectMsg(OutdatedLocationsAdded(outdatedLocations))
      }

      enterBarrier("after-add-outdated-locations")

      metadataCoordinator ! GetOutdatedLocations

      awaitAssert{
        expectMsgType[OutdatedLocationsGot].locations.toSet shouldBe outdatedLocations.toSet
      }

      enterBarrier("after-get-outdated-locations")
    }

    "manage errors when there are not enough cluster nodes" in {
      val selfMember = cluster.selfMember
      val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
      val metadataCoordinator = system.actorSelection(metadataCoordinatorPath(nodeName))

      cluster.leave(node(node2).address)
      awaitAssert {
        cluster.state.members.filter(_.status == MemberStatus.Up).map(_.address) shouldBe Set(node(node1).address)
        NSDbClusterSnapshot(system).nodes.size shouldBe 1
      }

      enterBarrier("one-node-up")

      val currentTime = System.currentTimeMillis()

      metadataCoordinator ! GetWriteLocations("db", "namespace", "metric", currentTime)

      awaitAssert{
        val reply = expectMsgType[GetWriteLocationsFailed]
        reply.db shouldBe "db"
        reply.namespace shouldBe "namespace"
        reply.metric shouldBe "metric"
        reply.reason shouldBe MetadataCoordinator.notEnoughReplicasErrorMessage(1, 2)
      }

      enterBarrier("after-GetWriteLocationsFailed-test")
    }

  }
}
