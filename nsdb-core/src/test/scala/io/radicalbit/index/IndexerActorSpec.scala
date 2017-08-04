package io.radicalbit.index
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.radicalbit.index.IndexerActor._
import io.radicalbit.model.Record
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

class IndexerActorSpec()
    extends TestKit(ActorSystem("MySpec"))
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

  "IndexerActor" should "write and delete properly record with compatible schemas" in {

    val record1 = Record(System.currentTimeMillis, Map("content" -> s"content"), Map.empty)
    val record2 = Record(System.currentTimeMillis, Map("content" -> s"content", "content2" -> s"content2"), Map.empty)
    val incompatibleRecord =
      Record(System.currentTimeMillis, Map("content" -> 1, "content2" -> s"content2"), Map.empty)

    probe.send(indexerActor, AddRecord("testMetric", record1))

    val expectedAdd = probe.expectMsgType[RecordAdded]
    expectedAdd.metric shouldBe "testMetric"
    expectedAdd.record shouldBe record1

    probe.send(indexerActor, GetSchema("testMetric"))
    val schema = probe.expectMsgType[SchemaGot]

    schema.schema.isDefined shouldBe true

    probe.send(indexerActor, AddRecord("testMetric", record2))

    val expectedAdd2 = probe.expectMsgType[RecordAdded]
    expectedAdd2.metric shouldBe "testMetric"
    expectedAdd2.record shouldBe record2

    probe.send(indexerActor, GetCount("testMetric"))

    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "testMetric"
    expectedCount.count shouldBe 2

    probe.send(indexerActor, AddRecord("testMetric", incompatibleRecord))

    probe.expectMsgType[RecordRejected]

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
