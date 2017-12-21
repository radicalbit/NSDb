package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Count
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands.GetLocations
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events.LocationsGot
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.InputMapped
import io.radicalbit.rtsae.STMultiNodeSpec

import scala.concurrent.duration._

object WriteCoordinatorClusterTest extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
    |akka.loglevel = ERROR
    |akka.actor.provider = "cluster"
    |akka.log-dead-letters-during-shutdown = off
    |nsdb{
    |
    |  read-coordinatoor.timeout = 10 seconds
    |  namespace-schema.timeout = 10 seconds
    |  namespace-data.timeout = 10 seconds
    |  publisher.timeout = 10 seconds
    |  publisher.scheduler.interval = 5 seconds
    |  write.scheduler.interval = 5 seconds
    |
    |  sharding {
    |    enabled = true
    |    interval = 1m
    |  }
    |  index.base-path = "target/test_index/WriteCoordinatorTest"
    |  write-coordinator.timeout = 5 seconds
    |  commit-log {
    |    enabled = false
    |  }
    |}""".stripMargin))
}

class WriteCoordinatorClusterTestMultiJvmNode1 extends WriteCoordinatorClusterTest

class WriteCoordinatorClusterTestMultiJvmNode2 extends WriteCoordinatorClusterTest

class WriteCoordinatorClusterTest
    extends MultiNodeSpec(WriteCoordinatorClusterTest)
    with STMultiNodeSpec
    with ImplicitSender {

  import WriteCoordinatorClusterTest._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)

  val guardian = system.actorOf(Props[DatabaseActorsGuardian], "guardian")

  val mediator = DistributedPubSub(system).mediator

  val metadataCoordinator = system.actorSelection("/user/guardian/metadata-coordinator")
  val writeCoordinator    = system.actorSelection("/user/guardian/write-coordinator")

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "WriteCoordinator" must {

    "join cluster" in within(10.seconds) {
      join(node1, node1)
      join(node2, node1)

      awaitAssert {
        mediator ! Count
        expectMsg(2)
      }

      enterBarrier("Joined")
    }

    "write records and update metadata" in within(10.seconds) {

      metadataCoordinator ! GetLocations("db", "namespace", "metric")
      val initialLoc = expectMsgType[LocationsGot]
      initialLoc.locations.size shouldBe 0

      runOn(node1) {
        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(0, 1.0, Map.empty))
        expectMsgType[InputMapped]
      }

      awaitAssert {
        metadataCoordinator ! GetLocations("db", "namespace", "metric")
        val locations = expectMsgType[LocationsGot]
        locations.locations.size shouldBe 1
        locations.locations.head.from shouldBe 0
        locations.locations.head.to shouldBe 60000
      }

      runOn(node2) {
        writeCoordinator ! MapInput(1, "db", "namespace", "metric", Bit(1, 1.0, Map.empty))
        expectMsgType[InputMapped]
      }

      awaitAssert {
        metadataCoordinator ! GetLocations("db", "namespace", "metric")
        val locations = expectMsgType[LocationsGot]
        locations.locations.size shouldBe 1
        locations.locations.head.from shouldBe 0
        locations.locations.head.to shouldBe 60000
      }

      runOn(node2) {
        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(60000, 1.0, Map.empty))
        expectMsgType[InputMapped]
      }

      awaitAssert {
        metadataCoordinator ! GetLocations("db", "namespace", "metric")
        val locations = expectMsgType[LocationsGot]
        locations.locations.size shouldBe 1
        locations.locations.head.from shouldBe 0
        locations.locations.head.to shouldBe 60000
      }
      runOn(node1) {
        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(30000, 1.0, Map.empty))
        expectMsgType[InputMapped]
      }
      runOn(node2) {
        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(40000, 1.0, Map.empty))
        expectMsgType[InputMapped]
      }

      awaitAssert {
        metadataCoordinator ! GetLocations("db", "namespace", "metric")
        val locations = expectMsgType[LocationsGot]
        locations.locations.size shouldBe 1
        locations.locations.head.from shouldBe 0
        locations.locations.head.to shouldBe 60000
      }

      enterBarrier("Single Location")

      runOn(node1) {
        writeCoordinator ! MapInput(60001, "db", "namespace", "metric", Bit(60001, 1.0, Map.empty))
        expectMsgType[InputMapped]
      }

      awaitAssert {

        metadataCoordinator ! GetLocations("db", "namespace", "metric")
        val locations = expectMsgType[LocationsGot]
        locations.locations.size shouldBe 2
        locations.locations.head.from shouldBe 0
        locations.locations.head.to shouldBe 60000
        locations.locations.last.from shouldBe 60001
        locations.locations.last.to shouldBe 120001
      }

      enterBarrier("Multiple Location")

    }

  }
}
