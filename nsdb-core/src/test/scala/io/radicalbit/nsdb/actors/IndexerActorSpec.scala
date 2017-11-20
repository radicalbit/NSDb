package io.radicalbit.nsdb.actors

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.radicalbit.nsdb.WriteInterval
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.NIOFSDirectory
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

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
    val paths = Seq(s"$basePath/$namespace/indexerActorMetric", s"$basePath/$namespace/indexerActorMetric2")

    paths.foreach { path =>
      val directory = new NIOFSDirectory(Paths.get(path))
      val writer    = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))
      writer.deleteAll()
      writer.flush()
      writer.close()
    }

  }

  "IndexerActor" should "write and delete properly" in {

    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))

    probe.send(indexerActor, AddRecord(db, namespace, "indexerActorMetric", bit))

    val expectedAdd = probe.expectMsgType[RecordAdded]
    expectedAdd.metric shouldBe "indexerActorMetric"
    expectedAdd.record shouldBe bit

    waitInterval

    probe.send(indexerActor, GetCount(db, namespace, "indexerActorMetric"))

    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "indexerActorMetric"
    expectedCount.count shouldBe 1

    probe.send(indexerActor, DeleteRecord(db, namespace, "indexerActorMetric", bit))

    val expectedDelete = probe.expectMsgType[RecordDeleted]
    expectedDelete.metric shouldBe "indexerActorMetric"
    expectedDelete.record shouldBe bit

    waitInterval

    probe.send(indexerActor, GetCount(db, namespace, "indexerActorMetric"))

    val expectedCountDeleted = probe.expectMsgType[CountGot]
    expectedCountDeleted.metric shouldBe "indexerActorMetric"
    expectedCountDeleted.count shouldBe 0

  }

  "IndexerActor" should "write and delete properly in multiple indexes" in {

    probe.send(indexerActor, DeleteAllMetrics(db, namespace))
    probe.expectMsgType[AllMetricsDeleted]

    waitInterval

    val bit = Bit(System.currentTimeMillis, 22.5, Map("content" -> "content"))

    probe.send(indexerActor, AddRecord(db, namespace, "indexerActorMetric2", bit))
    probe.expectMsgType[RecordAdded]

    waitInterval

    probe.send(indexerActor, GetCount(db, namespace, "indexerActorMetric"))
    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "indexerActorMetric"
    expectedCount.count shouldBe 0

    probe.send(indexerActor, GetCount(db, namespace, "indexerActorMetric2"))
    val expectedCount2 = probe.expectMsgType[CountGot]
    expectedCount2.metric shouldBe "indexerActorMetric2"
    expectedCount2.count shouldBe 1

  }

}
