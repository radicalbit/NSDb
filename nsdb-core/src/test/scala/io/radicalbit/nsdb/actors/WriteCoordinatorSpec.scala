package io.radicalbit.nsdb.actors

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.radicalbit.commit_log.CommitLogService.{Delete, Insert}
import io.radicalbit.coordinator.WriteCoordinator
import io.radicalbit.coordinator.WriteCoordinator.{GetSchema, MapInput, SchemaGot}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.index.IndexerActor
import io.radicalbit.nsdb.index.IndexerActor.{RecordAdded, RecordRejected}
import io.radicalbit.nsdb.model.Record
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class TestCommitLogService extends Actor {
  def receive = {
    case Insert(ts, metric, record) =>
      sender() ! WroteToCommitLogAck(ts, metric, record)
    case Delete(_, _) => sys.error("Not Implemented")
  }
}

class WriteCoordinatorSpec
    extends TestKit(ActorSystem("nsdb-test"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val probe      = TestProbe()
  val probeActor = probe.ref
  val writeCoordinatorActor = system actorOf WriteCoordinator.props(
    "target/test_index",
    system.actorOf(Props[TestCommitLogService]),
    system.actorOf(IndexerActor.props("target/test_index")))

  "WriteCoordinator" should "validate a schema" in {
    val record1 = Record(System.currentTimeMillis, Map("content" -> s"content"), Map.empty)
    val record2 = Record(System.currentTimeMillis, Map("content" -> s"content", "content2" -> s"content2"), Map.empty)
    val incompatibleRecord =
      Record(System.currentTimeMillis, Map("content" -> 1, "content2" -> s"content2"), Map.empty)

    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, "testMetric", record1))

    val expectedAdd = probe.expectMsgType[RecordAdded]
    expectedAdd.metric shouldBe "testMetric"
    expectedAdd.record shouldBe record1

    probe.send(writeCoordinatorActor, GetSchema("testMetric"))
    val schema = probe.expectMsgType[SchemaGot]

    schema.schema.isDefined shouldBe true

    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, "testMetric", record2))

    val expectedAdd2 = probe.expectMsgType[RecordAdded]
    expectedAdd2.metric shouldBe "testMetric"
    expectedAdd2.record shouldBe record2

    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, "testMetric", incompatibleRecord))

    probe.expectMsgType[RecordRejected]

  }

}
