/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType, TagFieldType}
import io.radicalbit.nsdb.common.statement.{AscOrderOperator, ListFields, SelectSQLStatement}
import io.radicalbit.nsdb.index.VARCHAR
import io.radicalbit.nsdb.model.{Location, Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class MetricAccumulatorActorSpec()
    extends TestKit(ActorSystem("metricAccumulatorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val indexingInterval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + 1.second
  val basePath  = "target/test_index"
  val db        = "db_shard"
  val namespace = "namespace"
  val metric    = "shardActorMetric"
  val nodeName  = "node1"

  val metricsReaderActor =
    system.actorOf(RoundRobinPool(1, None).props(MetricReaderActor.props(basePath, nodeName, db, namespace)))

  val metricAccumulatorActor =
    TestActorRef[MetricAccumulatorActor](
      MetricAccumulatorActor.props(basePath, db, namespace, metricsReaderActor, system.actorOf(Props.empty)),
      probeActor)


  val schema =
    Schema("",
      Set(SchemaField("dimension", DimensionFieldType, VARCHAR()), SchemaField("tag", TagFieldType, VARCHAR())))

  private val location = Location("testMetric", nodeName, 0, 0)


  private def selectAllOrderByTimestamp(metric: String) = ExecuteStatement(
    SelectSQLStatement(
      db = db,
      namespace = namespace,
      metric = metric,
      distinct = false,
      fields = ListFields(List.empty),
      condition = None,
      groupBy = None,
      order = Some(AscOrderOperator("timestamp"))
    )
  )

  private def checkExistance(location: Location): Boolean =
  Paths.get(basePath, db, namespace, "shards", s"${location.metric}_${location.from}_${location.to}").toFile.exists()

  before {
    implicit val timeout = Timeout(5 second)
    Await.result(metricAccumulatorActor ? DropMetricWithLocations(db, namespace, metric, Seq(location)), 5 seconds)
  }

  "MetricAccumulatorActor" should "write and delete properly" in {

    val bit      = Bit(System.currentTimeMillis, 25, Map("dimension" -> "dimension"), Map("tag" -> "tag"))
    val location = Location("shardActorMetric", nodeName, 0, 100)

    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, location, bit))
    awaitAssert {
      val expectedAdd = probe.expectMsgType[RecordAdded]
      expectedAdd.metric shouldBe metric
      expectedAdd.record shouldBe bit
    }
    expectNoMessage(indexingInterval)

    probe.send(metricsReaderActor, GetCountWithLocations(db, namespace, "shardActorMetric", Seq(location)))
    awaitAssert {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe metric
      expectedCount.count shouldBe 1
    }
    probe.send(metricAccumulatorActor, DeleteRecordFromShard(db, namespace, location, bit))
    within(5 seconds) {
      val expectedDelete = probe.expectMsgType[RecordDeleted]
      expectedDelete.metric shouldBe metric
      expectedDelete.record shouldBe bit
    }
    expectNoMessage(indexingInterval)

    probe.send(metricsReaderActor, GetCountWithLocations(db, namespace, "shardActorMetric", Seq(location)))
    awaitAssert {
      val expectedCountDeleted = probe.expectMsgType[CountGot]
      expectedCountDeleted.metric shouldBe metric
      expectedCountDeleted.count shouldBe 0
    }

  }

  "MetricAccumulatorActor" should "write and delete properly the same metric in multiple keys" in {

    val key  = Location("shardActorMetric", nodeName, 0, 100)
    val key2 = Location("shardActorMetric", nodeName, 101, 200)

    val bit11 = Bit(System.currentTimeMillis, 22.5, Map("dimension" -> "dimension"), Map("tag" -> "tag"))
    val bit12 = Bit(System.currentTimeMillis, 30.5, Map("dimension" -> "dimension"), Map("tag" -> "tag"))
    val bit13 = Bit(System.currentTimeMillis, 50.5, Map("dimension" -> "dimension"), Map("tag" -> "tag"))
    val bit21 = Bit(System.currentTimeMillis, 150, Map("dimension"  -> "dimension"), Map("tag" -> "tag"))
    val bit22 = Bit(System.currentTimeMillis, 160, Map("dimension"  -> "dimension"), Map("tag" -> "tag"))

    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, key, bit11))
    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, key, bit12))
    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, key, bit13))
    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, key2, bit21))
    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, key2, bit22))
    awaitAssert {
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
    }

    expectNoMessage(indexingInterval)

    probe.send(metricsReaderActor, GetCountWithLocations(db, namespace, "shardActorMetric", Seq(key, key2)))
    awaitAssert {
      val expectedCount = probe.expectMsgType[CountGot]

      expectedCount.metric shouldBe "shardActorMetric"
      expectedCount.count shouldBe 5

      metricAccumulatorActor.underlyingActor.shards.size shouldBe 2
      metricAccumulatorActor.underlyingActor.shards.keys.toSeq.contains(Location("shardActorMetric", nodeName, 0, 100))
      metricAccumulatorActor.underlyingActor.shards.keys.toSeq
        .contains(Location("shardActorMetric", nodeName, 101, 200))

      val i1     = metricAccumulatorActor.underlyingActor.shards(Location("shardActorMetric", nodeName, 0, 100))
      val shard1 = i1.all(schema)
      shard1.size shouldBe 3
      shard1 should contain(bit11)
      shard1 should contain(bit12)
      shard1 should contain(bit13)

      val i2     = metricAccumulatorActor.underlyingActor.shards(Location("shardActorMetric", nodeName, 101, 200))
      val shard2 = i2.all(schema)
      shard2.size shouldBe 2
      shard2 should contain(bit21)
      shard2 should contain(bit22)
    }
  }

  "MetricAccumulatorActor" should "drop a metric" in {

    val bit1 = Bit(System.currentTimeMillis, 25, Map("dimension" -> "dimension"), Map("tag" -> "tag"))
    val bit2 = Bit(System.currentTimeMillis, 30, Map("dimension" -> "dimension"), Map("tag" -> "tag"))

    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, location, bit1))
    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, location, bit2))
    probe.expectMsgType[RecordAdded]
    probe.expectMsgType[RecordAdded]

    expectNoMessage(indexingInterval)

    probe.send(metricsReaderActor, GetCountWithLocations(db, namespace, "testMetric", Seq(location)))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 2
    }

    probe.send(metricAccumulatorActor, DropMetricWithLocations(db, namespace, "testMetric", Seq(location)))
    within(5 seconds) {
      probe.expectMsgType[MetricDropped]
    }

    expectNoMessage(indexingInterval)

    probe.send(metricsReaderActor, GetCountWithLocations(db, namespace, "testMetric", Seq(location)))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 0
    }
  }

  "MetricAccumulatorActor" should "evict a location" in {

    metricAccumulatorActor.underlyingActor.shards.clear()
    metricAccumulatorActor.underlyingActor.facetIndexShards.clear()

    val bit1 = Bit(System.currentTimeMillis, 25, Map("dimension" -> "dimension"), Map("tag" -> "tag"))
    val bit2 = Bit(System.currentTimeMillis, 30, Map("dimension" -> "dimension"), Map("tag" -> "tag"))

    val location1 = Location("testMetric", nodeName, 25, 28)
    val location2 = Location("testMetric", nodeName, 30, 33)

    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, location1, bit1))
    probe.send(metricAccumulatorActor, AddRecordToShard(db, namespace, location2, bit2))
    probe.expectMsgType[RecordAdded]
    probe.expectMsgType[RecordAdded]

    expectNoMessage(indexingInterval)

    metricAccumulatorActor.underlyingActor.shards.size shouldBe 2
    metricAccumulatorActor.underlyingActor.facetIndexShards.size shouldBe 2

    checkExistance(location1) shouldBe true
    checkExistance(location2) shouldBe true

    probe.send(metricAccumulatorActor, EvictShard(db, namespace, location1))
    within(5 seconds) {
      probe.expectMsgType[ShardEvicted]
    }

    metricAccumulatorActor.underlyingActor.shards.size shouldBe 1
    metricAccumulatorActor.underlyingActor.facetIndexShards.size shouldBe 1

    checkExistance(location1) shouldBe false
    checkExistance(location2) shouldBe true
  }

}
