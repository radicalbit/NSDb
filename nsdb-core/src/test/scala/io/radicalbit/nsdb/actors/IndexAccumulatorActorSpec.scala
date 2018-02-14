package io.radicalbit.nsdb.actors

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

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
  val indexerAccumulatorActor =
    system.actorOf(IndexAccumulatorActor.props(basePath, db, namespace), "indexerAccumulatorActorTest")

  before {
    import scala.collection.JavaConverters._
    if (Paths.get(basePath, db).toFile.exists())
      Files.walk(Paths.get(basePath, db)).iterator().asScala.map(_.toFile).toSeq.reverse.foreach(_.delete)
  }

  "indexerAccumulatorActor" should "write and delete properly" in {

    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))

    probe.send(indexerAccumulatorActor, AddRecord(db, namespace, "indexerActorMetric", bit))
    within(5 seconds) {
      val expectedAdd = probe.expectMsgType[RecordAdded]
      expectedAdd.metric shouldBe "indexerActorMetric"
      expectedAdd.record shouldBe bit
    }
    expectNoMessage(interval)

    probe.send(indexerAccumulatorActor, GetCount(db, namespace, "indexerActorMetric"))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe "indexerActorMetric"
      expectedCount.count shouldBe 1
    }
    probe.send(indexerAccumulatorActor, DeleteRecord(db, namespace, "indexerActorMetric", bit))
    within(5 seconds) {
      val expectedDelete = probe.expectMsgType[RecordDeleted]
      expectedDelete.metric shouldBe "indexerActorMetric"
      expectedDelete.record shouldBe bit
    }
    expectNoMessage(interval)

    probe.send(indexerAccumulatorActor, GetCount(db, namespace, "indexerActorMetric"))
    within(5 seconds) {
      val expectedCountDeleted = probe.expectMsgType[CountGot]
      expectedCountDeleted.metric shouldBe "indexerActorMetric"
      expectedCountDeleted.count shouldBe 0
    }

  }

  "indexerAccumulatorActor" should "write and delete properly in multiple indexes" in {

    val bit = Bit(System.currentTimeMillis, 22.5, Map("content" -> "content"))

    probe.send(indexerAccumulatorActor, AddRecord(db, namespace, "indexerActorMetric2", bit))
    within(5 seconds) {
      probe.expectMsgType[RecordAdded]
    }

    expectNoMessage(interval)

    probe.send(indexerAccumulatorActor, GetCount(db, namespace, "indexerActorMetric"))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe "indexerActorMetric"
      expectedCount.count shouldBe 0
    }

    probe.send(indexerAccumulatorActor, GetCount(db, namespace, "indexerActorMetric2"))
    within(5 seconds) {
      val expectedCount2 = probe.expectMsgType[CountGot]
      expectedCount2.metric shouldBe "indexerActorMetric2"
      expectedCount2.count shouldBe 1
    }

  }

  "indexerAccumulatorActor" should "drop a metric" in {

    val bit1 = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    val bit2 = Bit(System.currentTimeMillis, 30, Map("content" -> "content"))

    probe.send(indexerAccumulatorActor, AddRecord("db", "testNamespace", "testMetric", bit1))
    probe.send(indexerAccumulatorActor, AddRecord("db", "testNamespace", "testMetric", bit2))
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
