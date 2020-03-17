package io.radicalbit.nsdb.cluster.actor

import akka.actor.{ActorSelection, Props}
import akka.cluster.MemberStatus
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.index.{BIGINT, DECIMAL, NumericType}
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.rtsae.STMultiNodeSpec

import scala.concurrent.Await
import scala.concurrent.duration._

object MetadataSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
    |akka.loglevel = ERROR
    |akka.actor {
    | provider = "cluster"
    |
    | serialization-bindings {
    |   "io.radicalbit.nsdb.common.protocol.NSDbSerializable" = jackson-json
    | }
    |
    | control-aware-dispatcher {
    |     mailbox-type = "akka.dispatch.UnboundedControlAwareMailbox"
    |   }
    |}
    |akka.log-dead-letters-during-shutdown = off
    |nsdb {
    |
    |  grpc {
    |    interface = "0.0.0.0"
    |    port = 7817
    |  }
    |
    |  global.timeout = 30 seconds
    |  read-coordinator.timeout = 10 seconds
    |  namespace-schema.timeout = 10 seconds
    |  namespace-data.timeout = 10 seconds
    |  rpc-endpoint.timeout = 30 seconds
    |  publisher.timeout = 10 seconds
    |  publisher.scheduler.interval = 5 seconds
    |  write.scheduler.interval = 15 seconds
    |  retention.check.interval = 1 seconds
    |
    |  cluster {
    |    metadata-write-consistency = "all"
    |    replication-factor = 2
    |    consistency-level = 2
    |  }
    |
    |  sharding {
    |    interval = 1d
    |    passivate-after = 1h
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
    |  storage {
    |    base-path  = "target/test_index/MetadataTest"
    |    index-path = ${nsdb.storage.base-path}"/index"
    |    commit-log-path = ${nsdb.storage.base-path}"/commit_log"
    |    metadata-path = ${nsdb.storage.base-path}"/metadata"
    |  }
    |
    |  write-coordinator.timeout = 5 seconds
    |  metadata-coordinator.timeout = 5 seconds
    |  commit-log {
    |    serializer = "io.radicalbit.nsdb.commit_log.StandardCommitLogSerializer"
    |    writer = "io.radicalbit.nsdb.commit_log.RollingCommitLogFileWriter"
    |    directory = "target/commitLog"
    |    max-size = 50000
    |    passivate-after = 5s
    |  }
    |  websocket {
    |    refresh-period = 100
    |    retention-size = 10
    |  }
    |}
    """.stripMargin))

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

  system.actorOf(Props[ClusterListenerTestActor], name = "clusterListener")

  lazy val nodeName = s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"

  "Metadata system" must {

    "join cluster" in {
      join(node1, node1)
      join(node2, node1)

      awaitAssert {
        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe 2
      }

      enterBarrier("joined")
    }

    "add location from different nodes" in {

      runOn(node1) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = Await.result(
          system.actorSelection(s"/user/guardian_$nodeName/metadata-coordinator_$nodeName").resolveOne(5.seconds),
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

        val metadataCoordinator = system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName")

        awaitAssert {
          metadataCoordinator ! PutMetricInfo(metricInfo)
          expectMsg(MetricInfoPut(metricInfo))
        }
      }

      enterBarrier("after-add-metrics-info")
    }

    "restore values from a metadata dump" in {

      runOn(node1) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName")

        val path = getClass.getResource("/dump").getPath

        metadataCoordinator ! ExecuteRestoreMetadata(path)

        awaitAssert {
          expectMsg(MetadataRestored(path))
        }
      }

      enterBarrier("metadata-restored")

      def checkCoordinates(metadataCoordinator: ActorSelection) = {
        awaitAssert {
          metadataCoordinator ! GetDbs
          expectMsg(DbsGot(Set("testDb", "testDbWithInfo", "db")))
        }

        awaitAssert {
          metadataCoordinator ! GetNamespaces("testDb")
          expectMsg(NamespacesGot("testDb", Set("testNamespace", "testNamespace2")))
        }

        awaitAssert {
          metadataCoordinator ! GetNamespaces("testDbWithInfo")
          expectMsg(NamespacesGot("testDbWithInfo", Set("testNamespaceWithInfo")))
        }

        awaitAssert {
          metadataCoordinator ! GetMetrics("testDb", "testNamespace")
          expectMsg(MetricsGot("testDb", "testNamespace", Set("people", "animals")))
        }

        awaitAssert {
          metadataCoordinator ! GetMetrics("testDb", "testNamespace2")
          expectMsg(MetricsGot("testDb", "testNamespace2", Set("animals")))
        }

        awaitAssert {
          metadataCoordinator ! GetMetrics("testDbWithInfo", "testNamespaceWithInfo")
          expectMsg(MetricsGot("testDbWithInfo", "testNamespaceWithInfo", Set("people")))
        }
      }

      def checkMetricInfoes(metadataCoordinator: ActorSelection) = {
        awaitAssert {
          metadataCoordinator ! GetMetricInfo("testDb", "testNamespace", "people")
          expectMsg(MetricInfoGot("testDb", "testNamespace", "people", None))

          metadataCoordinator ! GetMetricInfo("testDb", "testNamespace", "animals")
          expectMsg(MetricInfoGot("testDb", "testNamespace", "animals", None))
        }

        awaitAssert {
          metadataCoordinator ! GetMetricInfo("testDb", "testNamespace2", "animals")
          expectMsg(MetricInfoGot("testDb", "testNamespace2", "animals", None))
        }

        awaitAssert {
          metadataCoordinator ! GetMetricInfo("testDbWithInfo", "testNamespaceWithInfo", "people")
          expectMsg(
            MetricInfoGot("testDbWithInfo", "testNamespaceWithInfo", "people",
                          Some(MetricInfo("testDbWithInfo", "testNamespaceWithInfo", "people", 172800000, 86400000))))
        }
      }

      def checkSchemas(schemaCoordinator: ActorSelection) = {

        def dummyBit(valueType: NumericType[_]): Bit =
          Bit(0, valueType.zero, Map(
            "city"             -> "city",
            "bigDecimalLong"   -> 0L,
            "bigDecimalDouble" -> 1.1)
            , Map("gender"           -> "gender"))

        awaitAssert {
          schemaCoordinator ! GetSchema("testDb", "testNamespace", "people")
          expectMsg(
            SchemaGot(
              "testDb", "testNamespace", "people",
              Some(Schema("people", dummyBit(DECIMAL())))
            ))

          schemaCoordinator ! GetSchema("testDb", "testNamespace", "animals")
          expectMsg(
            SchemaGot(
              "testDb", "testNamespace", "animals",
              Some(Schema("animals", dummyBit(BIGINT())))
            ))
        }

        awaitAssert {
          schemaCoordinator ! GetSchema("testDb", "testNamespace2", "animals")
          expectMsg(
            SchemaGot(
              "testDb", "testNamespace2", "animals",
              Some(Schema("animals", dummyBit(BIGINT())))
            ))
        }

        awaitAssert {
          schemaCoordinator ! GetSchema("testDbWithInfo", "testNamespaceWithInfo", "people")
          expectMsg(
            SchemaGot(
              "testDbWithInfo", "testNamespaceWithInfo", "people",
              Some(Schema("people", dummyBit(BIGINT())))
            ))
        }
      }

      runOn(node1) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName")
        val schemaCoordinator   = system.actorSelection(s"user/guardian_$nodeName/schema-coordinator_$nodeName")

        checkCoordinates(metadataCoordinator)
        checkMetricInfoes(metadataCoordinator)
        checkSchemas(schemaCoordinator)

      }

      runOn(node2) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName")
        val schemaCoordinator   = system.actorSelection(s"user/guardian_$nodeName/schema-coordinator_$nodeName")

        checkCoordinates(metadataCoordinator)
        checkMetricInfoes(metadataCoordinator)
        checkSchemas(schemaCoordinator)

      }

    }

    "get write locations" in {

      val selfMember = cluster.selfMember
      val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

      val metadataCoordinator = system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName")

      metadataCoordinator ! GetWriteLocations("db", "namespace", "metric", 0)

      awaitAssert{
        val reply = expectMsgType[WriteLocationsGot]
        reply.db shouldBe "db"
        reply.namespace shouldBe "namespace"
        reply.metric shouldBe "metric"
        reply.locations.size shouldBe 2
      }

      enterBarrier("after-get-write-locations")

      cluster.leave(node(node2).address)
      awaitAssert {
        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe 1
      }

      enterBarrier("one-node-up")

      metadataCoordinator ! GetWriteLocations("db", "namespace", "metric", 0)

      awaitAssert{
        val reply = expectMsgType[GetWriteLocationsFailed]
        reply.db shouldBe "db"
        reply.namespace shouldBe "namespace"
        reply.metric shouldBe "metric"
        reply.reason shouldBe MetadataCoordinator.notEnoughReplicasErrorMessage(1, 2)
      }
    }
  }
}
