package io.radicalbit.nsdb.cluster

import akka.actor.{ActorSelection, Props}
import akka.cluster.MemberStatus
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.STMultiNodeSpec
import io.radicalbit.nsdb.cluster.actor.MetadataSpec.{node1, node2}
import io.radicalbit.nsdb.cluster.actor.{ClusterListenerTestActor, DatabaseActorsGuardian}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.ExecuteRestoreMetadata
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.MetadataRestored
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.{BIGINT, DECIMAL, NumericType}
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

object MetadataRestoreSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseResources("application.conf"))

  nodeConfig(node1)(ConfigFactory.parseString("akka.remote.artery.canonical.port = 25520"))

  nodeConfig(node2)(ConfigFactory.parseString("akka.remote.artery.canonical.port = 25530"))
}


class MetadataRestoreSpecMultiJvmNode1 extends MetadataRestoreSpec {}

class MetadataRestoreSpecMultiJvmNode2 extends MetadataRestoreSpec {}

class MetadataRestoreSpec extends MultiNodeSpec(MetadataRestoreSpec) with STMultiNodeSpec with ImplicitSender {
  override def initialParticipants: Int = roles.size

  private def metadataCoordinatorPath(nodeName: String) = s"user/guardian_${nodeName}/metadata-coordinator_${nodeName}_$nodeName"
  private def schemaCoordinatorPath(nodeName: String) = s"user/guardian_${nodeName}/schema-coordinator_${nodeName}_$nodeName"

 system.actorOf(Props[DatabaseActorsGuardian], "guardian")

  system.actorOf(ClusterListenerTestActor.props(), name = "clusterListener")

  "Metadata system" must {

    "join cluster" in {
      join(node1, node1)
      join(node2, node1)

      awaitAssert {
        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe roles.size
      }

      enterBarrier("joined")
    }

    "restore values from a metadata dump" in {

      runOn(node1) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = system.actorSelection(metadataCoordinatorPath(nodeName))

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
          expectMsg(DbsGot(Set("testDb", "testDbWithInfo")))
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

        val metadataCoordinator = system.actorSelection(metadataCoordinatorPath(nodeName))
        val schemaCoordinator   = system.actorSelection(schemaCoordinatorPath(nodeName))

        checkCoordinates(metadataCoordinator)
        checkMetricInfoes(metadataCoordinator)
        checkSchemas(schemaCoordinator)

      }

      runOn(node2) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = system.actorSelection(metadataCoordinatorPath(nodeName))
        val schemaCoordinator   = system.actorSelection(schemaCoordinatorPath(nodeName))

        checkCoordinates(metadataCoordinator)
        checkMetricInfoes(metadataCoordinator)
        checkSchemas(schemaCoordinator)

      }

    }

  }
}
