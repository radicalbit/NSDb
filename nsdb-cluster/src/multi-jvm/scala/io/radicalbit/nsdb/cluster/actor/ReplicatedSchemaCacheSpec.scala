package io.radicalbit.nsdb.cluster.actor

import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Replicator.{GetReplicaCount, ReplicaCount}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.ReplicatedSchemaCache._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{EvictSchema, GetSchemaFromCache, PutSchemaInCache}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SchemaCached
import io.radicalbit.rtsae.STMultiNodeSpec
import org.json4s.DefaultFormats

import scala.concurrent.duration._

object ReplicatedSchemaCacheSpec extends MultiNodeConfig {
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
    |  write.scheduler.interval = 15 seconds
    |
    |  index.base-path = "target/test_index/ReplicatedCacheSpec"
    |  write-coordinator.timeout = 5 seconds
    |  metadata-coordinator.timeout = 5 seconds
    |  commit-log {
    |    enabled = false
    |  }
    |}
    """.stripMargin))
}

class ReplicatedSchemaCacheSpecMultiJvmNode1 extends ReplicatedSchemaCacheSpec

class ReplicatedSchemaCacheSpecMultiJvmNode2 extends ReplicatedSchemaCacheSpec

class ReplicatedSchemaCacheSpec
    extends MultiNodeSpec(ReplicatedSchemaCacheSpec)
    with STMultiNodeSpec
    with ImplicitSender {

  import ReplicatedMetadataCacheSpec._

  implicit val formats = DefaultFormats

  override def initialParticipants = roles.size

  val cluster         = Cluster(system)
  val replicatedCache = system.actorOf(Props[ReplicatedSchemaCache])

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }

    enterBarrier(from.name + "-joined")
  }

  "ReplicatedSchemaCacheSpec" must {

    "join cluster" in within(20.seconds) {
      join(node1, node1)
      join(node2, node1)

      expectNoMessage(1 second)

      awaitAssert {
        DistributedData(system).replicator ! GetReplicaCount
        expectMsgType[ReplicaCount].n shouldBe 2
      }

      enterBarrier("joined")
    }

    "replicate cached entry" in within(5.seconds) {

      val db        = "db"
      val namespace = "namespace"

      val metric = "metric1"
      val key    = SchemaKey(db, namespace, metric)
      val schema = Schema(metric, Bit(0, 1L, Map("dimension" -> "dimension"), Map("tag" -> "tag"))).get

      runOn(node1) {
        awaitAssert {
          replicatedCache ! PutSchemaInCache(db, namespace, metric, schema)
          expectMsg(SchemaCached(db, namespace, metric, Some(schema)))
        }
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetSchemaFromCache(db, namespace, metric)
          expectMsg(SchemaCached(db, namespace, metric, Some(schema)))
        }
      }
      enterBarrier("after-add-schema")

    }

    "replicate many cached entries" in within(5.seconds) {

      val db        = "db"
      val namespace = "namespace"

      val metric      = "metric2"
      def key(i: Int) = SchemaKey(db, namespace, s"${metric}_$i")
      val schema      = Schema(metric, Bit(0, 1L, Map("dimension" -> "dimension"), Map("tag" -> "tag"))).get

      runOn(node1) {
        for (i ← 10 to 20) {
          replicatedCache ! PutSchemaInCache(key(i).db, key(i).namespace, key(i).metric, schema)
          expectMsg(SchemaCached(key(i).db, key(i).namespace, key(i).metric, Some(schema)))
        }
      }

      runOn(node2) {
        awaitAssert {
          for (i ← 10 to 20) {
            replicatedCache ! GetSchemaFromCache(key(i).db, key(i).namespace, key(i).metric)
            expectMsg(SchemaCached(key(i).db, key(i).namespace, key(i).metric, Some(schema)))
          }
        }
      }
      enterBarrier("after-build-add")
    }

    "replicate evicted entry" in within(5.seconds) {

      val db        = "db"
      val namespace = "namespace"

      val metric = "metric3"
      val key    = SchemaKey(db, namespace, metric)
      val schema = Schema(metric, Bit(0, 1L, Map("dimension" -> "dimension"), Map("tag" -> "tag"))).get

      runOn(node1) {
        replicatedCache ! PutSchemaInCache(db, namespace, metric, schema)
        expectMsg(SchemaCached(db, namespace, metric, Some(schema)))
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetSchemaFromCache(db, namespace, metric)
          expectMsg(SchemaCached(db, namespace, metric, Some(schema)))
        }

        replicatedCache ! EvictSchema(db, namespace, metric)
        expectMsg(SchemaCached(db, namespace, metric, None))
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetSchemaFromCache(db, namespace, metric)
          expectMsg(SchemaCached(db, namespace, metric, None))
        }
      }
      enterBarrier("after-eviction")
    }

    "replicate updated cached entry" in within(5.seconds) {

      val db        = "db"
      val namespace = "namespace"

      val metric = "metric4"

      val schema = Schema(metric, Bit(0, 1L, Map("dimension" -> "dimension"), Map("tag" -> "tag"))).get
      val updatedSchema =
        Schema("updatedMetric", Bit(0, 1L, Map("dimension" -> "dimension1"), Map("tag" -> "tag1"))).get

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

  }

}
