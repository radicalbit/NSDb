package io.radicalbit.nsdb.index

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.radicalbit.nsdb.index.IndexerActor._
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

    probe.send(indexerActor, AddRecord("testMetric", record))

    val expectedAdd = probe.expectMsgType[RecordAdded]
    expectedAdd.metric shouldBe "testMetric"
    expectedAdd.record shouldBe record

    probe.send(indexerActor, GetCount("testMetric"))

    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "testMetric"
    expectedCount.count shouldBe 1

    probe.send(indexerActor, DeleteRecord("testMetric", record))

    val expectedDelete = probe.expectMsgType[RecordDeleted]
    expectedAdd.metric shouldBe "testMetric"
    expectedAdd.record shouldBe record

    probe.send(indexerActor, GetCount("testMetric"))

    val expectedCountDeleted = probe.expectMsgType[CountGot]
    expectedCountDeleted.metric shouldBe "testMetric"
    expectedCountDeleted.count shouldBe 0

  }

  "IndexerActorSpec" should "write and delete properly in multiple indexes" in {

    probe.send(indexerActor, DeleteMetric("testMetric"))
    probe.expectMsgType[MetricDeleted]
    probe.send(indexerActor, DeleteMetric("testMetric2"))
    probe.expectMsgType[MetricDeleted]

    val record = Record(System.currentTimeMillis, Map("content" -> s"content"), Map.empty)

    probe.send(indexerActor, AddRecord("testMetric2", record))
    probe.expectMsgType[RecordAdded]

    probe.send(indexerActor, GetCount("testMetric"))
    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "testMetric"
    expectedCount.count shouldBe 0

    probe.send(indexerActor, GetCount("testMetric2"))
    val expectedCount2 = probe.expectMsgType[CountGot]
    expectedCount2.metric shouldBe "testMetric2"
    expectedCount2.count shouldBe 1

  }

}
