package io.radicalbit.nsdb.cluster.actor

import akka.actor.Props
import akka.cluster.MemberStatus
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
import io.radicalbit.nsdb.model.{Location, LocationWithCoordinates}

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

  val guardian = system.actorOf(Props[DatabaseActorsGuardian], "guardian")

  implicit val timeout: Timeout = Timeout(5.seconds)

  system.actorOf(ClusterListenerTestActor.props(), name = "clusterListener")

  private def metadataCoordinatorPath(nodeName: String) = s"user/guardian_${nodeName}_$nodeName/metadata-coordinator_${nodeName}_$nodeName"

  "Metadata system" must {

    "join cluster" in {
      cluster.join(node(node1).address)

      awaitAssert {
        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe 2
        NSDbClusterSnapshot(system).nodes.size shouldBe 2
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
          metadataCoordinator ! AddLocations("db", "namespace", Seq(Location("metric", "node-1", 0, 1)))
          expectMsg(LocationsAdded("db", "namespace", Seq(Location("metric", "node-1", 0, 1))))
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
        LocationWithCoordinates("db", "namespace", Location("metric", "node1", 0,1)),
        LocationWithCoordinates("db1", "namespace", Location("metric1", "node2", 1,4)),
        LocationWithCoordinates("db", "namespace1", Location("metric", "node1", 0,1)),
        LocationWithCoordinates("db", "namespaces", Location("metric", "node2", 0,1))
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
