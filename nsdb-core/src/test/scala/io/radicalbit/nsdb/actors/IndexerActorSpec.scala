package io.radicalbit.nsdb.actors

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.IndexerActor._
import io.radicalbit.nsdb.actors.NamespaceActor._
import io.radicalbit.nsdb.model.Record
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

class IndexerActorSpec()
    extends TestKit(ActorSystem("IndexerActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe        = TestProbe()
  val probeActor   = probe.ref
  val indexerActor = system.actorOf(IndexerActor.props("target/test_index"))

  override def before(fun: => Any)(implicit pos: Position): Unit = {
    super.before(fun)

    val paths = Seq("target/test_index/testMetric", "target/test_index/testMetric2")

    paths.foreach { path =>
      val directory = FSDirectory.open(Paths.get(path))
      val writer    = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer))
      writer.deleteAll()
      writer.flush()
      writer.close()
    }
  }

  "IndexerActor" should "write and delete properly" in {

    val record = Record(System.currentTimeMillis, Map("content" -> s"content"), Map.empty)

    probe.send(indexerActor, AddRecord("namespace", "indexerActorMetric", record))

    val expectedAdd = probe.expectMsgType[RecordAdded]
    expectedAdd.metric shouldBe "indexerActorMetric"
    expectedAdd.record shouldBe record

    probe.send(indexerActor, GetCount("namespace", "indexerActorMetric"))

    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "indexerActorMetric"
    expectedCount.count shouldBe 1

    probe.send(indexerActor, DeleteRecord("namespace", "indexerActorMetric", record))

    val expectedDelete = probe.expectMsgType[RecordDeleted]
    expectedAdd.metric shouldBe "indexerActorMetric"
    expectedAdd.record shouldBe record

    probe.send(indexerActor, GetCount("namespace", "indexerActorMetric"))

    val expectedCountDeleted = probe.expectMsgType[CountGot]
    expectedCountDeleted.metric shouldBe "indexerActorMetric"
    expectedCountDeleted.count shouldBe 0

  }

  "IndexerActorSpec" should "write and delete properly in multiple indexes" in {

    probe.send(indexerActor, DeleteMetric("namespace", "indexerActorMetric"))
    probe.expectMsgType[MetricDeleted]
    probe.send(indexerActor, DeleteMetric("namespace", "indexerActorMetric2"))
    probe.expectMsgType[MetricDeleted]

    val record = Record(System.currentTimeMillis, Map("content" -> s"content"), Map.empty)

    probe.send(indexerActor, AddRecord("namespace", "indexerActorMetric2", record))
    probe.expectMsgType[RecordAdded]

    probe.send(indexerActor, GetCount("namespace", "indexerActorMetric"))
    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "indexerActorMetric"
    expectedCount.count shouldBe 0

    probe.send(indexerActor, GetCount("namespace", "indexerActorMetric2"))
    val expectedCount2 = probe.expectMsgType[CountGot]
    expectedCount2.metric shouldBe "indexerActorMetric2"
    expectedCount2.count shouldBe 1

  }

}
