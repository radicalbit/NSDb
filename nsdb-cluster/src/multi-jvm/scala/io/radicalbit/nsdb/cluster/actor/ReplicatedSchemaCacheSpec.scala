package io.radicalbit.nsdb.cluster.actor

import akka.actor.Props
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Replicator.{GetReplicaCount, ReplicaCount}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.ReplicatedSchemaCache._
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.commands.DeleteNamespaceSchema
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.events.NamespaceSchemaDeleted
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{EvictSchema, GetSchemaFromCache, PutSchemaInCache}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SchemaCached
import io.radicalbit.nsdb.NSDbMultiNodeSpec
import io.radicalbit.nsdb.cluster.MetadataSpec.commonConfig
import org.json4s.DefaultFormats

import scala.concurrent.duration._

object ReplicatedSchemaCacheSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseResources("application.conf"))

}

class ReplicatedSchemaCacheSpecMultiJvmNode1 extends ReplicatedSchemaCacheSpec

class ReplicatedSchemaCacheSpecMultiJvmNode2 extends ReplicatedSchemaCacheSpec

class ReplicatedSchemaCacheSpec
    extends MultiNodeSpec(ReplicatedSchemaCacheSpec)
    with NSDbMultiNodeSpec
    with ImplicitSender {

  import ReplicatedMetadataCacheSpec._

  implicit val formats = DefaultFormats

  override def initialParticipants = roles.size

  val replicatedCache = system.actorOf(Props[ReplicatedSchemaCache])

  val db        = "db"
  val namespace = "namespace"

  val metric1 = "metric1"
  val key1    = SchemaKey(db, namespace, metric1)
  val schema1 = Schema(metric1, Bit(0, 1L, Map("dimension" -> "dimension"), Map("tag" -> "tag")))

  val metric2 = "metric2"
  val key2    = SchemaKey(db, namespace, metric2)
  val schema2 = Schema(metric2, Bit(0, 1.5, Map("dimension" -> "dimension"), Map("tag" -> "tag")))

  val metric3 = "metric3"
  val key3    = SchemaKey(db, namespace, metric3)
  val schema3 = Schema(metric3, Bit(0, 1L, Map("dimension" -> "dimension"), Map("tag" -> "tag")))

  "ReplicatedSchemaCacheSpec" must {

    "join cluster" in within(20.seconds) {
      join(node1, node1)
      join(node2, node1)

      awaitAssert {
        DistributedData(system).replicator ! GetReplicaCount
        expectMsgType[ReplicaCount].n shouldBe 2
      }

      enterBarrier("joined")
    }

    "replicate cached entry" in within(5.seconds) {

      awaitAssert {
        replicatedCache ! GetSchemaFromCache(db, namespace, metric1)
        expectMsg(SchemaCached(db, namespace, metric1, None))
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! PutSchemaInCache(db, namespace, metric1, schema1)
          expectMsg(SchemaCached(db, namespace, metric1, Some(schema1)))
        }
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetSchemaFromCache(db, namespace, metric1)
          expectMsg(SchemaCached(db, namespace, metric1, Some(schema1)))
        }
      }
      enterBarrier("after-add-schema")

    }

    "replicate many cached entries" in within(5.seconds) {

      def key(i: Int) = SchemaKey(db, namespace, s"multimetric_$i")
      def schema(i: Int) =
        Schema(s"multimetric_$i", Bit(0, 1L, Map("dimension" -> "dimension"), Map("tag" -> "tag")))

      runOn(node1) {
        for (i ← 10 to 20) {
          replicatedCache ! PutSchemaInCache(key(i).db, key(i).namespace, key(i).metric, schema(i))
          expectMsg(SchemaCached(key(i).db, key(i).namespace, key(i).metric, Some(schema(i))))
        }
      }

      runOn(node2) {
        awaitAssert {
          for (i ← 10 to 20) {
            replicatedCache ! GetSchemaFromCache(key(i).db, key(i).namespace, key(i).metric)
            expectMsg(SchemaCached(key(i).db, key(i).namespace, key(i).metric, Some(schema(i))))
          }
        }
      }
      enterBarrier("after-build-add")
    }

    "replicate evicted entry" in within(5.seconds) {
      runOn(node1) {
        replicatedCache ! PutSchemaInCache(db, namespace, metric2, schema2)
        expectMsg(SchemaCached(db, namespace, metric2, Some(schema2)))

        replicatedCache ! PutSchemaInCache(db, namespace, metric3, schema3)
        expectMsg(SchemaCached(db, namespace, metric3, Some(schema3)))
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetSchemaFromCache(db, namespace, metric2)
          expectMsg(SchemaCached(db, namespace, metric2, Some(schema2)))

          replicatedCache ! GetSchemaFromCache(db, namespace, metric3)
          expectMsg(SchemaCached(db, namespace, metric3, Some(schema3)))
        }

        replicatedCache ! EvictSchema(db, namespace, metric3)
        expectMsg(SchemaCached(db, namespace, metric3, None))
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetSchemaFromCache(db, namespace, metric2)
          expectMsg(SchemaCached(db, namespace, metric2, Some(schema2)))

          replicatedCache ! GetSchemaFromCache(db, namespace, metric3)
          expectMsg(SchemaCached(db, namespace, metric3, None))
        }
      }
      enterBarrier("after-eviction")
    }

    "replicate updated cached entry" in within(5.seconds) {

      val db        = "db"
      val namespace = "namespace"

      val metric = "metric4"

      val schema = Schema(metric, Bit(0, 1L, Map("dimension" -> "dimension"), Map("tag" -> "tag")))
      val updatedSchema =
        Schema("updatedMetric", Bit(0, 1L, Map("dimension" -> "dimension1"), Map("tag" -> "tag1")))

      runOn(node1) {
        replicatedCache ! PutSchemaInCache(db, namespace, metric, schema)
        expectMsg(SchemaCached(db, namespace, metric, Some(schema)))
      }

      runOn(node2) {
        replicatedCache ! PutSchemaInCache(db, namespace, metric, updatedSchema)
        expectMsg(SchemaCached(db, namespace, metric, Some(updatedSchema)))
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetSchemaFromCache(db, namespace, metric)
          expectMsg(SchemaCached(db, namespace, metric, Some(updatedSchema)))
        }
      }
      enterBarrier("after-update")
    }

    "delete a namespace and allow to reinsert schemas" in within(5.seconds) {
      runOn(node1) {
        replicatedCache ! PutSchemaInCache(db, namespace, metric2, schema2)
        expectMsg(SchemaCached(db, namespace, metric2, Some(schema2)))

        replicatedCache ! PutSchemaInCache(db, namespace, metric3, schema3)
        expectMsg(SchemaCached(db, namespace, metric3, Some(schema3)))
      }

      awaitAssert {
        replicatedCache ! GetSchemaFromCache(db, namespace, metric2)
        expectMsg(SchemaCached(db, namespace, metric2, Some(schema2)))

        replicatedCache ! GetSchemaFromCache(db, namespace, metric3)
        expectMsg(SchemaCached(db, namespace, metric3, Some(schema3)))
      }

      runOn(node2) {
        replicatedCache ! DeleteNamespaceSchema(db, namespace)
        expectMsg(NamespaceSchemaDeleted(db, namespace))
      }

      awaitAssert {
        replicatedCache ! GetSchemaFromCache(db, namespace, metric2)
        expectMsg(SchemaCached(db, namespace, metric2, None))

        replicatedCache ! GetSchemaFromCache(db, namespace, metric3)
        expectMsg(SchemaCached(db, namespace, metric3, None))
      }

      enterBarrier("after-namespace-deletion")

      runOn(node1) {
        replicatedCache ! PutSchemaInCache(db, namespace, metric2, schema2)
        expectMsg(SchemaCached(db, namespace, metric2, Some(schema2)))

        replicatedCache ! PutSchemaInCache(db, namespace, metric3, schema3)
        expectMsg(SchemaCached(db, namespace, metric3, Some(schema3)))
      }

      awaitAssert {
        replicatedCache ! GetSchemaFromCache(db, namespace, metric2)
        expectMsg(SchemaCached(db, namespace, metric2, Some(schema2)))

        replicatedCache ! GetSchemaFromCache(db, namespace, metric3)
        expectMsg(SchemaCached(db, namespace, metric3, Some(schema3)))
      }

      enterBarrier("after-reinsertion")
    }
  }

}
