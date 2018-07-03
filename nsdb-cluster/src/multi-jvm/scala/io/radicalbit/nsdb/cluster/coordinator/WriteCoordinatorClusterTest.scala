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
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.GetLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsGot
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{InputMapped, WarmUpCompleted}
import io.radicalbit.rtsae.STMultiNodeSpec
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._

object WriteCoordinatorClusterTest extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
    |akka.loglevel = ERROR
    |akka.actor{
    | provider = "cluster"
    | control-aware-dispatcher {
    |     mailbox-type = "akka.dispatch.UnboundedControlAwareMailbox"
    |   }
    |}
    |akka.log-dead-letters-during-shutdown = off
    |nsdb{
    |
    |  read-coordinator.timeout = 10 seconds
    |  namespace-schema.timeout = 10 seconds
    |  namespace-data.timeout = 10 seconds
    |  publisher.timeout = 10 seconds
    |  publisher.scheduler.interval = 5 seconds
    |  write.scheduler.interval = 5 seconds
    |
    |  sharding {
    |    interval = 1m
    |  }
    |
    |  read {
    |    parallelism {
    |      initial-size = 1
    |      lower-bound= 1
    |      upper-bound = 1
    |    }
    |  }
    |
    |  index.base-path = "target/test_index/WriteCoordinatorTest"
    |  write-coordinator.timeout = 10 seconds
    |  metadata-coordinator.timeout = 10 seconds
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
    with ImplicitSender
    with BeforeAndAfterAll {

  import WriteCoordinatorClusterTest._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)

  val guardian = system.actorOf(Props[DatabaseActorsGuardian], "guardian")

  val mediator = DistributedPubSub(system).mediator

  val metadataCoordinator =
    system.actorSelection("/user/guardian/metadata-coordinator")
  val writeCoordinator =
    system.actorSelection("/user/guardian/write-coordinator")

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
      val initialLoc = awaitAssert {
        expectMsgType[LocationsGot]
      }
      initialLoc.locations.size shouldBe 0

      Thread.sleep(200)

      runOn(node1) {
        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(0, 1.0, Map.empty, Map.empty))
        awaitAssert {
          expectMsgType[InputMapped]
        }
      }

      Thread.sleep(500)

      metadataCoordinator ! GetLocations("db", "namespace", "metric")
      val locations_1 = awaitAssert {
        expectMsgType[LocationsGot]
      }
      locations_1.locations.size shouldBe 1
      locations_1.locations.head.from shouldBe 0
      locations_1.locations.head.to shouldBe 60000

      Thread.sleep(200)

      runOn(node2) {
        writeCoordinator ! MapInput(1, "db", "namespace", "metric", Bit(1, 1.0, Map.empty, Map.empty))
        awaitAssert {
          expectMsgType[InputMapped]
        }
      }

      metadataCoordinator ! GetLocations("db", "namespace", "metric")
      val locations_2 = awaitAssert {
        expectMsgType[LocationsGot]
      }
      locations_2.locations.size shouldBe 1
      locations_2.locations.head.from shouldBe 0
      locations_2.locations.head.to shouldBe 60000

      runOn(node2) {
        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(50000, 1.0, Map.empty, Map.empty))
        expectMsgType[InputMapped]
      }

      metadataCoordinator ! GetLocations("db", "namespace", "metric")
      val locations_3 = awaitAssert {
        expectMsgType[LocationsGot]
      }
      locations_3.locations.size shouldBe 1
      locations_3.locations.head.from shouldBe 0
      locations_3.locations.head.to shouldBe 60000

      runOn(node1) {
        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(30000, 1.0, Map.empty, Map.empty))
        expectMsgType[InputMapped]
      }
      runOn(node2) {
        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(40000, 1.0, Map.empty, Map.empty))
        expectMsgType[InputMapped]
      }

      metadataCoordinator ! GetLocations("db", "namespace", "metric")
      val locations_4 = awaitAssert {
        expectMsgType[LocationsGot]
      }
      locations_4.locations.size shouldBe 1
      locations_4.locations.head.from shouldBe 0
      locations_4.locations.head.to shouldBe 60000

      enterBarrier("Single Location")

      runOn(node1) {
        writeCoordinator ! MapInput(60001, "db", "namespace", "metric", Bit(60001, 1.0, Map.empty, Map.empty))
        expectMsgType[InputMapped]
      }

      Thread.sleep(200)

      metadataCoordinator ! GetLocations("db", "namespace", "metric")
      val locations_5 = awaitAssert {
        expectMsgType[LocationsGot]
      }
      locations_5.locations.size shouldBe 2
      locations_5.locations.head.from shouldBe 0
      locations_5.locations.head.to shouldBe 60000
      locations_5.locations.last.from shouldBe 60000
      locations_5.locations.last.to shouldBe 120000

      enterBarrier("Multiple Location")

    }

  }
}
