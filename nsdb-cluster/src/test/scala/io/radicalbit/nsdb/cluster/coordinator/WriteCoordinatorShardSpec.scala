package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.PublisherActor.Command.SubscribeBySqlStatement
import io.radicalbit.nsdb.actors.PublisherActor.Events.SubscribedByQueryString
import io.radicalbit.nsdb.cluster.actor.{NamespaceDataActor, NamespaceSchemaActor}
import io.radicalbit.nsdb.cluster.coordinator.Facilities._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class WriteCoordinatorShardSpec
    extends TestKit(
      ActorSystem(
        "nsdb-test",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.enabled", ConfigValueFactory.fromAnyRef(true))
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
      ))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val basePath             = "target/test_index/WriteCoordinatorShardSpec"
  val probe                = TestProbe()
  val probeActor           = probe.ref
  val namespaceSchemaActor = TestActorRef[NamespaceSchemaActor](NamespaceSchemaActor.props(basePath))
  val namespaceDataActor   = TestActorRef[NamespaceDataActor](NamespaceDataActor.props(basePath))
  val subscriber           = TestActorRef[TestSubscriber](Props[TestSubscriber])
  val publisherActor =
    TestActorRef[PublisherActor](
      PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor]), namespaceSchemaActor))
  val fakeMetadataCoordinator = system.actorOf(Props[FakeMetadataCoordinator])
  val writeCoordinatorActor = system actorOf WriteCoordinator.props(fakeMetadataCoordinator,
                                                                    namespaceSchemaActor,
                                                                    Some(system.actorOf(Props[TestCommitLogService])),
                                                                    publisherActor)

  val db        = "writeCoordinatorSpecShardDB"
  val namespace = "namespace"

  val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"))
  val record2 = Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"))

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS)

  before {
    import akka.pattern.ask

    import scala.concurrent.duration._

    implicit val timeout = Timeout(3 seconds)

    Await.result(namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, "testMetric", record1), 3 seconds)
    Await.result(writeCoordinatorActor ? SubscribeNamespaceDataActor(namespaceDataActor, Some("node1")), 3 seconds)
  }

  "WriteCoordinator in shard mode" should "write records" in within(5.seconds) {
    val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"))
    val record2 = Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"))
    val incompatibleRecord =
      Bit(System.currentTimeMillis, 3, Map("content" -> 1, "content2" -> s"content2"))

    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record1))

    val expectedAdd = probe.expectMsgType[InputMapped]
    expectedAdd.metric shouldBe "testMetric"
    expectedAdd.record shouldBe record1

    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record2))

    val expectedAdd2 = probe.expectMsgType[InputMapped]
    expectedAdd2.metric shouldBe "testMetric"
    expectedAdd2.record shouldBe record2

    probe.send(writeCoordinatorActor,
               MapInput(System.currentTimeMillis, db, namespace, "testMetric", incompatibleRecord))

    probe.expectMsgType[RecordRejected]

  }

  "WriteCoordinator in shard mode" should "write records and publish event to its subscriber" in within(5.seconds) {
    val testRecordSatisfy = Bit(100, 1, Map("name" -> "john"))

    val testSqlStatement = SelectSQLStatement(
      db = db,
      namespace = namespace,
      metric = "testMetric",
      distinct = false,
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

    awaitAssert {
      val expectedAdd = probe.expectMsgType[InputMapped]
      expectedAdd.metric shouldBe "testMetric"
      expectedAdd.record shouldBe testRecordSatisfy

      subscriber.underlyingActor.receivedMessages shouldBe 1
    }
  }

  "WriteCoordinator in shard mode" should "delete a namespace" in within(5.seconds) {
    probe.send(writeCoordinatorActor, DeleteNamespace(db, namespace))

    awaitAssert {
      probe.expectMsgType[NamespaceDeleted]

      namespaceSchemaActor.underlyingActor.schemaActors.keys.size shouldBe 0
      namespaceDataActor.underlyingActor.childActors.keys.size shouldBe 0
    }
  }

  "WriteCoordinator in shard mode" should "delete entries" in within(5.seconds) {

    val records: Seq[Bit] = Seq(
      Bit(2, 1, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
      Bit(4, 1, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
      Bit(6, 1, Map("name"  -> "Bill", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
      Bit(8, 1, Map("name"  -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis())),
      Bit(10, 1, Map("name" -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()))
    )

    records.foreach(r => probe.send(writeCoordinatorActor, MapInput(r.timestamp, db, "testDelete", "testMetric", r)))

    awaitAssert {
      (0 to 4) foreach { _ =>
        probe.expectMsgType[InputMapped]
      }
    }

    expectNoMessage(interval)

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

    awaitAssert {
      probe.expectMsgType[DeleteStatementExecuted]
    }
  }

  "WriteCoordinator" should "drop a metric" in within(5.seconds) {
    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record1))
    probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record2))

    probe.expectMsgType[InputMapped]
    probe.expectMsgType[InputMapped]

    expectNoMessage(interval)
    expectNoMessage(interval)

    probe.send(namespaceSchemaActor, GetSchema(db, namespace, "testMetric"))
    probe.expectMsgType[SchemaGot].schema.isDefined shouldBe true

    probe.send(namespaceDataActor, GetCount(db, namespace, "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[CountGot].count shouldBe 2
    }

    probe.send(writeCoordinatorActor, DropMetric(db, namespace, "testMetric"))
    within(5 seconds) {
      probe.expectMsgType[MetricDropped]
    }

    expectNoMessage(interval)

    probe.send(namespaceDataActor, GetCount(db, namespace, "testMetric"))

    awaitAssert {
      probe.expectMsgType[CountGot].count shouldBe 0
    }

    probe.send(namespaceSchemaActor, GetSchema(db, namespace, "testMetric"))
    probe.expectMsgType[SchemaGot].schema.isDefined shouldBe false
  }

}
