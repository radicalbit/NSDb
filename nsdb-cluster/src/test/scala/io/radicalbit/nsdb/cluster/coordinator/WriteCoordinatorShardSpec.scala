package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.actors.{NamespaceSchemaActor, PublisherActor}
import io.radicalbit.nsdb.actors.PublisherActor.Command.SubscribeBySqlStatement
import io.radicalbit.nsdb.actors.PublisherActor.Events.SubscribedByQueryString
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor
import io.radicalbit.nsdb.cluster.coordinator.Facilities._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class WriteCoordinatorShardSpec
    extends TestKit(
      ActorSystem("nsdb-test",
                  ConfigFactory
                    .load()
                    .withValue("nsdb.sharding.enabled", ConfigValueFactory.fromAnyRef(true))))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val basePath             = "target/test_index/WriteCoordinatorShardSpec"
  val probe                = TestProbe()
  val probeActor           = probe.ref
  val namespaceSchemaActor = TestActorRef[NamespaceSchemaActor](NamespaceSchemaActor.props(basePath))
  val namespaceDataActor   = TestActorRef[NamespaceDataActor](NamespaceDataActor.props(basePath))
  val subscriber           = TestActorRef[TestSubscriber](Props[TestSubscriber])
  val publisherActor =
    TestActorRef[PublisherActor](PublisherActor.props(basePath, system.actorOf(Props[FakeReadCoordinatorActor]), namespaceSchemaActor))
  val fakeMetadataCoordinator = system.actorOf(Props[FakeMetadataCoordinator])
  val writeCoordinatorActor = system actorOf WriteCoordinator.props(fakeMetadataCoordinator,
                                                                    namespaceSchemaActor,
                                                                    Some(system.actorOf(Props[TestCommitLogService])),
                                                                    publisherActor)

  val db        = "writeCoordinatorSpecShardDB"
  val namespace = "namespace"
  
  val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"))
  val record2 = Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"))

  override def beforeAll() = {
    import akka.pattern.ask

    import scala.concurrent.duration._

    implicit val timeout = Timeout(3 seconds)


    Await.result(namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, "testMetric", record1), 3 seconds)
    Await.result(writeCoordinatorActor ? SubscribeNamespaceDataActor(namespaceDataActor, Some("node1")), 3 seconds)
  }

  "WriteCoordinator in shard mode" should "write records" in {
    val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"))
    val record2 = Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"))
    val incompatibleRecord =
      Bit(System.currentTimeMillis, 3, Map("content" -> 1, "content2" -> s"content2"))

    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, "testNamespace", "testMetric", record1))

    val expectedAdd = probe.expectMsgType[InputMapped]
    expectedAdd.metric shouldBe "testMetric"
    expectedAdd.record shouldBe record1

    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, "testNamespace", "testMetric", record2))

    val expectedAdd2 = probe.expectMsgType[InputMapped]
    expectedAdd2.metric shouldBe "testMetric"
    expectedAdd2.record shouldBe record2

    probe.send(writeCoordinatorActor,
               MapInput(System.currentTimeMillis, db, "testNamespace", "testMetric", incompatibleRecord))

    probe.expectMsgType[RecordRejected]

  }

  "WriteCoordinator in shard mode" should "write records and publish event to its subscriber" in {
    val testRecordSatisfy = Bit(100, 1, Map("name" -> "john"))

    val testSqlStatement = SelectSQLStatement(
      db = db,
      namespace = namespace,
      metric = "testMetric",
      fields = AllFields,
      condition = Some(
        Condition(ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
      limit = Some(LimitOperator(4))
    )

    probe.send(publisherActor, SubscribeBySqlStatement(subscriber, "testQueryString", testSqlStatement))
    probe.expectMsgType[SubscribedByQueryString]
    publisherActor.underlyingActor.subscribedActors.keys.size shouldBe 1
    publisherActor.underlyingActor.queries.keys.size shouldBe 1

    probe.send(writeCoordinatorActor,
               MapInput(System.currentTimeMillis, db, namespace, "testMetric", testRecordSatisfy))

    within(5 seconds) {
      val expectedAdd = probe.expectMsgType[InputMapped]
      expectedAdd.metric shouldBe "testMetric"
      expectedAdd.record shouldBe testRecordSatisfy

      subscriber.underlyingActor.receivedMessages shouldBe 1
    }
  }

  "WriteCoordinator in shard mode" should "delete a namespace" in {
    probe.send(writeCoordinatorActor, DeleteNamespace(db, "testNamespace"))

    within(5 seconds) {
      probe.expectMsgType[NamespaceDeleted]

      namespaceSchemaActor.underlyingActor.schemaActors.keys.size shouldBe 0
      namespaceDataActor.underlyingActor.childActors.keys.size shouldBe 0
    }
  }

  "WriteCoordinator in shard mode" should "delete entries" in {

    val records: Seq[Bit] = Seq(
      Bit(2, 1, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
      Bit(4, 1, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
      Bit(6, 1, Map("name"  -> "Bill", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
      Bit(8, 1, Map("name"  -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis())),
      Bit(10, 1, Map("name" -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()))
    )

    records.foreach(r =>
      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, "testDelete", "testMetric", r)))

    within(5 seconds) {
      (0 to 4) foreach { _ =>
        probe.expectMsgType[InputMapped]
      }
    }

    probe.send(
      writeCoordinatorActor,
      ExecuteDeleteStatement(
        DeleteSQLStatement(
          db = db,
          namespace = "testDelete",
          metric = "testMetric",
          condition = Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))
        )
      )
    )
    within(5 seconds) {
      probe.expectMsgType[DeleteStatementExecuted]
    }
  }

  "WriteCoordinator" should "drop a metric" in {
    probe.send(writeCoordinatorActor, DropMetric(db, "testNamespace", "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[MetricDropped]
    }
  }

}
