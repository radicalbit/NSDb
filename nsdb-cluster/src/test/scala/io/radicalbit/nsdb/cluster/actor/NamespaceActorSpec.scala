package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor.{AddRecordToLocation, DeleteRecordFromLocation}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class NamespaceActorSpec()
    extends TestKit(ActorSystem("namespaceActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe          = TestProbe()
  val probeActor     = probe.ref
  val basePath       = "target/test_index/NamespaceActorSpec"
  val db             = "db"
  val namespace      = "namespace"
  val namespace1     = "namespace1"
  val namespaceActor = system.actorOf(NamespaceDataActor.props(basePath))

  private val metric = "namespaceActorMetric"

  val location = Location(_: String, "testNode", 0, 0)

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + (1 second)

  before {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = 10 second

    Await.result(namespaceActor ? DeleteNamespace(db, namespace), 3 seconds)
    Await.result(namespaceActor ? DeleteNamespace(db, namespace1), 3 seconds)
  }

  "namespaceActor" should "write and delete properly" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 0.5, Map("content" -> s"content"))

    probe.send(namespaceActor, AddRecordToLocation(db, namespace, record, location(metric)))

    awaitAssert {
      val expectedAdd = probe.expectMsgType[RecordAdded]
      expectedAdd.metric shouldBe metric
      expectedAdd.record shouldBe record
    }

    expectNoMessage(interval)

    probe.send(namespaceActor, GetCount(db, namespace, metric))

    awaitAssert {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe metric
      expectedCount.count shouldBe 1
    }

    probe.send(namespaceActor, DeleteRecordFromLocation(db, namespace, metric, record, location(metric)))

    awaitAssert {
      val expectedDelete = probe.expectMsgType[RecordDeleted]
      expectedDelete.metric shouldBe metric
      expectedDelete.record shouldBe record
    }

    expectNoMessage(interval)

    probe.send(namespaceActor, GetCount(db, namespace, metric))

    awaitAssert {
      val expectedCountDeleted = probe.expectMsgType[CountGot]
      expectedCountDeleted.metric shouldBe metric
      expectedCountDeleted.count shouldBe 0
    }

  }

  "namespaceActor" should "write and delete properly in multiple namespaces" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 24, Map("content" -> s"content"))

    probe.send(namespaceActor, AddRecordToLocation(db, namespace1, record, location(metric + "2")))
    probe.expectMsgType[RecordAdded]

    expectNoMessage(interval)

    probe.send(namespaceActor, GetCount(db, namespace, metric))

    awaitAssert {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe metric
      expectedCount.count shouldBe 0
    }

    probe.send(namespaceActor, GetCount(db, namespace1, metric + "2"))

    awaitAssert {
      val expectedCount2 = probe.expectMsgType[CountGot]
      expectedCount2.metric shouldBe metric + "2"
      expectedCount2.count shouldBe 1
    }

  }

  "namespaceActor" should "delete a namespace" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 23, Map("content" -> s"content"))

    probe.send(namespaceActor, AddRecordToLocation(db, namespace1, record, location(metric + "2")))
    probe.expectMsgType[RecordAdded]

    expectNoMessage(interval)

    probe.send(namespaceActor, GetCount(db, namespace1, metric + "2"))
    awaitAssert {
      val expectedCount2 = probe.expectMsgType[CountGot]
      expectedCount2.metric shouldBe metric + "2"
      expectedCount2.count shouldBe 1
    }

    probe.send(namespaceActor, DeleteNamespace(db, namespace1))
    awaitAssert {
      probe.expectMsgType[NamespaceDeleted]
    }

    expectNoMessage(interval)

  }

}
