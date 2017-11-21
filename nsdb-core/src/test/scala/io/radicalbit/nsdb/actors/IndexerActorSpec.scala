package io.radicalbit.nsdb.actors

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.radicalbit.nsdb.WriteInterval
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class IndexerActorSpec()
    extends TestKit(ActorSystem("IndexerActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter
    with WriteInterval {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val basePath     = "target/test_index"
  val db           = "db"
  val namespace    = "namespace"
  val indexerActor = system.actorOf(IndexerActor.props(basePath, db, namespace))

  before {
    import scala.collection.JavaConverters._
    if (Paths.get(basePath, db).toFile.exists())
      Files.walk(Paths.get(basePath, db)).iterator().asScala.map(_.toFile).toSeq.reverse.foreach(_.delete)
  }

  "IndexerActor" should "write and delete properly" in {

    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))

    probe.send(indexerActor, AddRecord(db, namespace, "indexerActorMetric", bit))
    within(5 seconds) {
      val expectedAdd = probe.expectMsgType[RecordAdded]
      expectedAdd.metric shouldBe "indexerActorMetric"
      expectedAdd.record shouldBe bit
    }
    waitInterval

    probe.send(indexerActor, GetCount(db, namespace, "indexerActorMetric"))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe "indexerActorMetric"
      expectedCount.count shouldBe 1
    }
    probe.send(indexerActor, DeleteRecord(db, namespace, "indexerActorMetric", bit))
    within(5 seconds) {
      val expectedDelete = probe.expectMsgType[RecordDeleted]
      expectedDelete.metric shouldBe "indexerActorMetric"
      expectedDelete.record shouldBe bit
    }
    waitInterval

    probe.send(indexerActor, GetCount(db, namespace, "indexerActorMetric"))
    within(5 seconds) {
      val expectedCountDeleted = probe.expectMsgType[CountGot]
      expectedCountDeleted.metric shouldBe "indexerActorMetric"
      expectedCountDeleted.count shouldBe 0
    }

  }

  "IndexerActor" should "write and delete properly in multiple indexes" in {

//    probe.send(indexerActor, DeleteAllMetrics(db, namespace))
//    within(5 seconds) {
//      probe.expectMsgType[AllMetricsDeleted]
//    }
//
//    waitInterval

    val bit = Bit(System.currentTimeMillis, 22.5, Map("content" -> "content"))

    probe.send(indexerActor, AddRecord(db, namespace, "indexerActorMetric2", bit))
    within(5 seconds) {
      probe.expectMsgType[RecordAdded]
    }

    waitInterval

    probe.send(indexerActor, GetCount(db, namespace, "indexerActorMetric"))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe "indexerActorMetric"
      expectedCount.count shouldBe 0
    }

    probe.send(indexerActor, GetCount(db, namespace, "indexerActorMetric2"))
    within(5 seconds) {
      val expectedCount2 = probe.expectMsgType[CountGot]
      expectedCount2.metric shouldBe "indexerActorMetric2"
      expectedCount2.count shouldBe 1
    }

  }

  "IndexerActor" should "drop a metric" in {

    val bit1 = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    val bit2 = Bit(System.currentTimeMillis, 30, Map("content" -> "content"))

    probe.send(indexerActor, AddRecord("db", "testNamespace", "testMetric", bit1))
    probe.send(indexerActor, AddRecord("db", "testNamespace", "testMetric", bit2))
    probe.expectMsgType[RecordAdded]
    probe.expectMsgType[RecordAdded]

    waitInterval

    probe.send(indexerActor, GetCount("db", "testNamespace", "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 2
    }

    probe.send(indexerActor, DropMetric("db", "testNamespace", "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[MetricDropped]
    }

    waitInterval

    probe.send(indexerActor, GetCount("db", "testNamespace", "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 0
    }
  }

}
