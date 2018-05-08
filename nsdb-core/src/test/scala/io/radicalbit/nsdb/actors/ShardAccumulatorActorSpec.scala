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

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class ShardAccumulatorActorSpec()
    extends TestKit(ActorSystem("shardActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + 1.second
  val basePath  = "target/test_index"
  val db        = "db_shard"
  val namespace = "namespace"
  val metric    = "shardActorMetric"
  val shardActor =
    TestActorRef[ShardAccumulatorActor](ShardAccumulatorActor.props(basePath, db, namespace), probeActor)

  before {
    implicit val timeout = Timeout(5 second)
    Await.result(shardActor ? DropMetric(db, namespace, metric), 5 seconds)
  }

  "ShardAccumulatorActor" should "write and delete properly" in {

    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    val key = ShardKey("shardActorMetric", 0, 100)

    probe.send(shardActor, AddRecordToShard(db, namespace, key, bit))
    awaitAssert {
      val expectedAdd = probe.expectMsgType[RecordAdded]
      expectedAdd.metric shouldBe metric
      expectedAdd.record shouldBe bit
    }
    expectNoMessage(interval)

    probe.send(shardActor, GetCount(db, namespace, "shardActorMetric"))
    awaitAssert {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe metric
      expectedCount.count shouldBe 1
    }
    probe.send(shardActor, DeleteRecordFromShard(db, namespace, key, bit))
    within(5 seconds) {
      val expectedDelete = probe.expectMsgType[RecordDeleted]
      expectedDelete.metric shouldBe metric
      expectedDelete.record shouldBe bit
    }
    expectNoMessage(interval)

    probe.send(shardActor, GetCount(db, namespace, "shardActorMetric"))
    awaitAssert {
      val expectedCountDeleted = probe.expectMsgType[CountGot]
      expectedCountDeleted.metric shouldBe metric
      expectedCountDeleted.count shouldBe 0
    }

  }

  "ShardAccumulatorActor" should "write and delete properly the same metric in multiple keys" in {

    val key  = ShardKey("shardActorMetric", 0, 100)
    val key2 = ShardKey("shardActorMetric", 101, 200)

    val bit11 = Bit(System.currentTimeMillis, 22.5, Map("content" -> "content"))
    val bit12 = Bit(System.currentTimeMillis, 30.5, Map("content" -> "content"))
    val bit13 = Bit(System.currentTimeMillis, 50.5, Map("content" -> "content"))
    val bit21 = Bit(System.currentTimeMillis, 150, Map("content"  -> "content"))
    val bit22 = Bit(System.currentTimeMillis, 160, Map("content"  -> "content"))

    probe.send(shardActor, AddRecordToShard(db, namespace, key, bit11))
    probe.send(shardActor, AddRecordToShard(db, namespace, key, bit12))
    probe.send(shardActor, AddRecordToShard(db, namespace, key, bit13))
    probe.send(shardActor, AddRecordToShard(db, namespace, key2, bit21))
    probe.send(shardActor, AddRecordToShard(db, namespace, key2, bit22))
    awaitAssert {
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
    }

    expectNoMessage(interval)

    probe.send(shardActor, GetCount(db, namespace, "shardActorMetric"))
    awaitAssert {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe "shardActorMetric"
      expectedCount.count shouldBe 5

      shardActor.underlyingActor.shards.size shouldBe 2
      shardActor.underlyingActor.shards.keys.toSeq.contains(ShardKey("shardActorMetric", 0, 100))
      shardActor.underlyingActor.shards.keys.toSeq.contains(ShardKey("shardActorMetric", 101, 200))

      val i1     = shardActor.underlyingActor.shards(ShardKey("shardActorMetric", 0, 100))
      val shard1 = i1.all
      shard1.size shouldBe 3
      shard1 should contain(bit11)
      shard1 should contain(bit12)
      shard1 should contain(bit13)

      val i2     = shardActor.underlyingActor.shards(ShardKey("shardActorMetric", 101, 200))
      val shard2 = i2.all
      shard2.size shouldBe 2
      shard2 should contain(bit21)
      shard2 should contain(bit22)
    }
  }

  "indexerAccumulatorActor" should "drop a metric" in {

    val bit1 = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    val bit2 = Bit(System.currentTimeMillis, 30, Map("content" -> "content"))

    probe.send(shardActor, AddRecordToShard(db, namespace, ShardKey("testMetric", 0, 0), bit1))
    probe.send(shardActor, AddRecordToShard(db, namespace, ShardKey("testMetric", 0, 0), bit2))
    probe.expectMsgType[RecordAdded]
    probe.expectMsgType[RecordAdded]

    expectNoMessage(interval)

    probe.send(shardActor, GetCount(db, namespace, "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 2
    }

    probe.send(shardActor, DropMetric(db, namespace, "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[MetricDropped]
    }

    expectNoMessage(interval)

    probe.send(shardActor, GetCount(db, namespace, "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 0
    }
  }

}
