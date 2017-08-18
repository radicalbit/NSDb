package io.radicalbit.nsdb.actors

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.actors.NamespaceActor._
import io.radicalbit.nsdb.model.Record
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.Await

class NamespaceActorSpec()
    extends TestKit(ActorSystem("namespaceActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe          = TestProbe()
  val probeActor     = probe.ref
  val basePath       = "target/test_index"
  val namespaceActor = TestActorRef[NamespaceActor](NamespaceActor.props(basePath))

  before {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = 1 second

    Await.result(namespaceActor ? DeleteNamespace("namespace"), 1 seconds)
    Await.result(namespaceActor ? DeleteNamespace("namespace1"), 1 seconds)
  }

  "namespaceActor" should "write and delete properly" in {

    val record = Record(System.currentTimeMillis, Map("content" -> s"content"), Map.empty)

    probe.send(namespaceActor, AddRecord("namespace", "namespaceActorMetric", record))

    val expectedAdd = probe.expectMsgType[RecordAdded]
    expectedAdd.metric shouldBe "namespaceActorMetric"
    expectedAdd.record shouldBe record

    probe.send(namespaceActor, GetCount("namespace", "namespaceActorMetric"))

    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "namespaceActorMetric"
    expectedCount.count shouldBe 1

    probe.send(namespaceActor, DeleteRecord("namespace", "namespaceActorMetric", record))

    val expectedDelete = probe.expectMsgType[RecordDeleted]
    expectedDelete.metric shouldBe "namespaceActorMetric"
    expectedDelete.record shouldBe record

    probe.send(namespaceActor, GetCount("namespace", "namespaceActorMetric"))

    val expectedCountDeleted = probe.expectMsgType[CountGot]
    expectedCountDeleted.metric shouldBe "namespaceActorMetric"
    expectedCountDeleted.count shouldBe 0

  }

  "namespaceActorSpec" should "write and delete properly in multiple namespaces" in {

    val record = Record(System.currentTimeMillis, Map("content" -> s"content"), Map.empty)

    probe.send(namespaceActor, AddRecord("namespace1", "namespaceActorMetric2", record))
    probe.expectMsgType[RecordAdded]

    probe.send(namespaceActor, GetCount("namespace", "namespaceActorMetric"))
    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "namespaceActorMetric"
    expectedCount.count shouldBe 0

    probe.send(namespaceActor, GetCount("namespace1", "namespaceActorMetric2"))
    val expectedCount2 = probe.expectMsgType[CountGot]
    expectedCount2.metric shouldBe "namespaceActorMetric2"
    expectedCount2.count shouldBe 1

  }

  "namespaceActorSpec" should "delete a namespace" in {

    val record = Record(System.currentTimeMillis, Map("content" -> s"content"), Map.empty)

    probe.send(namespaceActor, AddRecord("namespace1", "namespaceActorMetric2", record))
    probe.expectMsgType[RecordAdded]

    probe.send(namespaceActor, GetCount("namespace1", "namespaceActorMetric2"))
    val expectedCount2 = probe.expectMsgType[CountGot]
    expectedCount2.metric shouldBe "namespaceActorMetric2"
    expectedCount2.count shouldBe 1

    namespaceActor.underlyingActor.indexerActors.keys.size shouldBe 1

    probe.send(namespaceActor, DeleteNamespace("namespace1"))
    probe.expectMsgType[NamespaceDeleted]

    namespaceActor.underlyingActor.indexerActors.keys.size shouldBe 0
  }

}
