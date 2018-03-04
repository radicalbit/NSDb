package io.radicalbit.nsdb.actors

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class IndexAccumulatorActorSpec()
    extends TestKit(ActorSystem("IndexerActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + 1.second

  val basePath  = "target/test_index"
  val db        = "db"
  val namespace = "namespace"
  val metric    = "indexerActorMetric"
  val shardKey  = ShardKey(_: String, 0, 0)

  val indexerAccumulatorActor =
    system.actorOf(ShardAccumulatorActor.props(basePath, db, namespace), "indexerAccumulatorActorTest")

  before {
    implicit val timeout = Timeout(5 second)
    Await.result(indexerAccumulatorActor ? DropMetric(db, namespace, metric), 5 seconds)
  }

  "indexerAccumulatorActor" should "write and delete properly" in {

    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))

    probe.send(indexerAccumulatorActor, AddRecordToShard(db, namespace, shardKey(metric), bit))
    within(5 seconds) {
      val expectedAdd = probe.expectMsgType[RecordAdded]
      expectedAdd.metric shouldBe metric
      expectedAdd.record shouldBe bit
    }
    expectNoMessage(interval)

    probe.send(indexerAccumulatorActor, GetCount(db, namespace, metric))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe metric
      expectedCount.count shouldBe 1
    }
    probe.send(indexerAccumulatorActor, DeleteRecordFromShard(db, namespace, shardKey(metric), bit))
    within(5 seconds) {
      val expectedDelete = probe.expectMsgType[RecordDeleted]
      expectedDelete.metric shouldBe metric
      expectedDelete.record shouldBe bit
    }
    expectNoMessage(interval)

    probe.send(indexerAccumulatorActor, GetCount(db, namespace, metric))
    within(5 seconds) {
      val expectedCountDeleted = probe.expectMsgType[CountGot]
      expectedCountDeleted.metric shouldBe metric
      expectedCountDeleted.count shouldBe 0
    }

  }

  "indexerAccumulatorActor" should "write and delete properly in multiple indexes" in {

    val bit = Bit(System.currentTimeMillis, 22.5, Map("content" -> "content"))

    probe.send(indexerAccumulatorActor, AddRecordToShard(db, namespace, shardKey(s"${metric}2"), bit))
    within(5 seconds) {
      probe.expectMsgType[RecordAdded]
    }

    expectNoMessage(interval)

    probe.send(indexerAccumulatorActor, GetCount(db, namespace, metric))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe metric
      expectedCount.count shouldBe 0
    }

    probe.send(indexerAccumulatorActor, GetCount(db, namespace, metric + "2"))
    within(5 seconds) {
      val expectedCount2 = probe.expectMsgType[CountGot]
      expectedCount2.metric shouldBe metric + "2"
      expectedCount2.count shouldBe 1
    }

  }

  "indexerAccumulatorActor" should "drop a metric" in {

    val bit1 = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    val bit2 = Bit(System.currentTimeMillis, 30, Map("content" -> "content"))

    probe.send(indexerAccumulatorActor, AddRecordToShard("db", "testNamespace", shardKey("testMetric"), bit1))
    probe.send(indexerAccumulatorActor, AddRecordToShard("db", "testNamespace", shardKey("testMetric"), bit2))
    probe.expectMsgType[RecordAdded]
    probe.expectMsgType[RecordAdded]

    expectNoMessage(interval)

    probe.send(indexerAccumulatorActor, GetCount("db", "testNamespace", "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 2
    }

    probe.send(indexerAccumulatorActor, DropMetric("db", "testNamespace", "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[MetricDropped]
    }

    expectNoMessage(interval)

    probe.send(indexerAccumulatorActor, GetCount("db", "testNamespace", "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 0
    }
  }

}
