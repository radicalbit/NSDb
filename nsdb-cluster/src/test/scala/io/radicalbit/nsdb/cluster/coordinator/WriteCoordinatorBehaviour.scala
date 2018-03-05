package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.actor.Props
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.PublisherActor.Command.SubscribeBySqlStatement
import io.radicalbit.nsdb.actors.PublisherActor.Events.SubscribedByQueryString
import io.radicalbit.nsdb.cluster.actor.{NamespaceDataActor, NamespaceSchemaActor}
import io.radicalbit.nsdb.cluster.coordinator.Facilities.{
  FakeMetadataCoordinator,
  FakeReadCoordinatorActor,
  TestCommitLogService,
  TestSubscriber
}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{Matchers, _}

import scala.concurrent.duration._

trait WriteCoordinatorBehaviour { this: TestKit with WordSpecLike with Matchers =>

  val probe = TestProbe()

  def basePath: String

  def db: String

  def namespace: String

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS)

  lazy val namespaceSchemaActor = TestActorRef[NamespaceSchemaActor](NamespaceSchemaActor.props(basePath))
  lazy val namespaceDataActor   = TestActorRef[NamespaceDataActor](NamespaceDataActor.props(basePath))
  lazy val subscriber           = TestActorRef[TestSubscriber](Props[TestSubscriber])
  lazy val publisherActor =
    TestActorRef[PublisherActor](
      PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor]), namespaceSchemaActor))
  lazy val fakeMetadataCoordinator = system.actorOf(Props[FakeMetadataCoordinator])
  lazy val writeCoordinatorActor = system actorOf WriteCoordinator.props(
    fakeMetadataCoordinator,
    namespaceSchemaActor,
    Some(system.actorOf(Props[TestCommitLogService])),
    publisherActor)

  val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"))
  val record2 = Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"))

  def defaultBehaviour {
    "write records" in {
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

    "write records and publish event to its subscriber" in {
      val testRecordSatisfy = Bit(100, 1, Map("name" -> "john"))

      val testSqlStatement = SelectSQLStatement(
        db = db,
        namespace = namespace,
        metric = "testMetric",
        distinct = false,
        fields = AllFields,
        condition = Some(
          Condition(
            ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
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

    "delete a namespace" in {
      probe.send(writeCoordinatorActor, DeleteNamespace(db, namespace))

      within(5 seconds) {
        probe.expectMsgType[NamespaceDeleted]

        namespaceSchemaActor.underlyingActor.schemaActors.keys.size shouldBe 0
        namespaceDataActor.underlyingActor.childActors.keys.size shouldBe 0
      }
//      expectNoMessage(interval)
    }

    "delete entries" in {

      val records: Seq[Bit] = Seq(
        Bit(2, 1, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
        Bit(4, 1, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
        Bit(6, 1, Map("name"  -> "Bill", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
        Bit(8, 1, Map("name"  -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis())),
        Bit(10, 1, Map("name" -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()))
      )

      records.foreach(r => probe.send(writeCoordinatorActor, MapInput(r.timestamp, db, "testDelete", "testMetric", r)))

      within(5 seconds) {
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
      within(5 seconds) {
        probe.expectMsgType[DeleteStatementExecuted]
      }

      expectNoMessage(interval)
    }

    "drop a metric" in {
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
      expectNoMessage(interval)

      probe.send(namespaceDataActor, GetCount(db, namespace, "testMetric"))
      within(5 seconds) {
        probe.expectMsgType[CountGot].count shouldBe 0
      }

      probe.send(namespaceSchemaActor, GetSchema(db, namespace, "testMetric"))
      probe.expectMsgType[SchemaGot].schema.isDefined shouldBe false
    }

  }
}
