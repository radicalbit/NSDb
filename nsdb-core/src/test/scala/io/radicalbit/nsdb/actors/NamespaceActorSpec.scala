package io.radicalbit.nsdb.actors

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.WriteInterval
import io.radicalbit.nsdb.actors.NamespaceDataActor.commands._
import io.radicalbit.nsdb.actors.NamespaceDataActor.events.{CountGot, GetCount, RecordAdded, RecordDeleted}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.coordinator.WriteCoordinator.{DeleteNamespace, NamespaceDeleted}
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.Await

class NamespaceActorSpec()
    extends TestKit(ActorSystem("namespaceActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter
    with WriteInterval {

  val probe          = TestProbe()
  val probeActor     = probe.ref
  val basePath       = "target/test_index"
  val namespace      = "namespace"
  val namespace1     = "namespace1"
  val namespaceActor = TestActorRef[NamespaceDataActor](NamespaceDataActor.props(basePath))

  before {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = 10 second

    Await.result(namespaceActor ? DeleteNamespace(namespace), 1 seconds)
    Await.result(namespaceActor ? DeleteNamespace(namespace1), 1 seconds)
  }

  "namespaceActor" should "write and delete properly" in {

    val record = Bit(System.currentTimeMillis, 0.5, Map("content" -> s"content"))

    probe.send(namespaceActor, AddRecord(namespace, "namespaceActorMetric", record))

    val expectedAdd = probe.expectMsgType[RecordAdded]
    expectedAdd.metric shouldBe "namespaceActorMetric"
    expectedAdd.record shouldBe record

    waitInterval

    probe.send(namespaceActor, GetCount(namespace, "namespaceActorMetric"))

    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "namespaceActorMetric"
    expectedCount.count shouldBe 1

    probe.send(namespaceActor, DeleteRecord(namespace, "namespaceActorMetric", record))

    val expectedDelete = probe.expectMsgType[RecordDeleted]
    expectedDelete.metric shouldBe "namespaceActorMetric"
    expectedDelete.record shouldBe record

    waitInterval

    probe.send(namespaceActor, GetCount(namespace, "namespaceActorMetric"))

    val expectedCountDeleted = probe.expectMsgType[CountGot]
    expectedCountDeleted.metric shouldBe "namespaceActorMetric"
    expectedCountDeleted.count shouldBe 0

  }

  "namespaceActor" should "write and delete properly in multiple namespaces" in {

    val record = Bit(System.currentTimeMillis, 24, Map("content" -> s"content"))

    probe.send(namespaceActor, AddRecord(namespace1, "namespaceActorMetric2", record))
    probe.expectMsgType[RecordAdded]

    waitInterval

    probe.send(namespaceActor, GetCount(namespace, "namespaceActorMetric"))
    val expectedCount = probe.expectMsgType[CountGot]
    expectedCount.metric shouldBe "namespaceActorMetric"
    expectedCount.count shouldBe 0

    probe.send(namespaceActor, GetCount(namespace1, "namespaceActorMetric2"))
    val expectedCount2 = probe.expectMsgType[CountGot]
    expectedCount2.metric shouldBe "namespaceActorMetric2"
    expectedCount2.count shouldBe 1

  }

  "namespaceActor" should "delete a namespace" in {

    val record = Bit(System.currentTimeMillis, 23, Map("content" -> s"content"))

    probe.send(namespaceActor, AddRecord(namespace1, "namespaceActorMetric2", record))
    probe.expectMsgType[RecordAdded]

    waitInterval

    probe.send(namespaceActor, GetCount(namespace1, "namespaceActorMetric2"))
    val expectedCount2 = probe.expectMsgType[CountGot]
    expectedCount2.metric shouldBe "namespaceActorMetric2"
    expectedCount2.count shouldBe 1

    namespaceActor.underlyingActor.indexerActors.keys.size shouldBe 1

    probe.send(namespaceActor, DeleteNamespace(namespace1))
    probe.expectMsgType[NamespaceDeleted]

    waitInterval

    namespaceActor.underlyingActor.indexerActors.keys.size shouldBe 0
  }

}
