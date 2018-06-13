/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.PublisherActor.Command.SubscribeBySqlStatement
import io.radicalbit.nsdb.actors.PublisherActor.Events.{RecordsPublished, SubscribedByQueryString}
import io.radicalbit.nsdb.cluster.actor.{MetricsDataActor, MetricsSchemaActor}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{GetLocations, GetWriteLocation}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{LocationGot, LocationsGot}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{Matchers, _}

import scala.collection.mutable
import scala.concurrent.duration._

class TestSubscriber extends Actor {
  var receivedMessages = 0
  def receive = {
    case RecordsPublished(_, _, _) =>
      receivedMessages += 1
  }
}

class FakeReadCoordinatorActor extends Actor {
  def receive: Receive = {
    case ExecuteStatement(_) =>
      sender() ! SelectStatementExecuted(db = "db",
                                         namespace = "testNamespace",
                                         metric = "testMetric",
                                         values = Seq.empty)
  }
}

class FakeMetadataCoordinator extends Actor with ActorLogging {

  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  val locations: mutable.Map[(String, String), Seq[Location]] = mutable.Map.empty

  override def receive: Receive = {
    case GetLocations(db, namespace, metric) =>
      sender() ! LocationsGot(db, namespace, metric, locations.getOrElse((namespace, metric), Seq.empty))
    case GetWriteLocation(db, namespace, metric, timestamp) =>
      val location = Location(metric, "node1", timestamp, timestamp + shardingInterval.toMillis)
      locations
        .get((namespace, metric))
        .fold {
          locations += (namespace, metric) -> Seq(location)
        } { oldSeq =>
          locations += (namespace, metric) -> (oldSeq :+ location)
        }
      sender() ! LocationGot(db, namespace, metric, Some(location))
  }
}

trait WriteCoordinatorBehaviour { this: TestKit with WordSpecLike with Matchers =>

  val probe = TestProbe()

  def basePath: String

  def db: String

  def namespace: String

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + 1.second

  lazy val namespaceSchemaActor = TestActorRef[MetricsSchemaActor](MetricsSchemaActor.props(basePath))
  lazy val metricsDataActor     = TestActorRef[MetricsDataActor](MetricsDataActor.props(basePath))
  lazy val subscriber           = TestActorRef[TestSubscriber](Props[TestSubscriber])
  lazy val publisherActor =
    TestActorRef[PublisherActor](PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor])))
  lazy val fakeMetadataCoordinator = system.actorOf(Props[FakeMetadataCoordinator])
  lazy val writeCoordinatorActor = system actorOf WriteCoordinator.props(None,
                                                                         fakeMetadataCoordinator,
                                                                         namespaceSchemaActor,
                                                                         publisherActor)

  val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"))
  val record2 = Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"))

  def defaultBehaviour {
    "write records" in within(5.seconds) {
      val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"))
      val record2 = Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"))
      val incompatibleRecord =
        Bit(System.currentTimeMillis, 3, Map("content" -> 1, "content2" -> s"content2"))

      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record1))

      val expectedAdd = awaitAssert {
        probe.expectMsgType[InputMapped]
      }
      expectedAdd.metric shouldBe "testMetric"
      expectedAdd.record shouldBe record1

      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record2))

      val expectedAdd2 = awaitAssert {
        probe.expectMsgType[InputMapped]
      }
      expectedAdd2.metric shouldBe "testMetric"
      expectedAdd2.record shouldBe record2

      probe.send(writeCoordinatorActor,
                 MapInput(System.currentTimeMillis, db, namespace, "testMetric", incompatibleRecord))

      awaitAssert {
        probe.expectMsgType[RecordRejected]
      }

    }

    "write records and publish event to its subscriber" in within(5.seconds) {
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
      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1
      publisherActor.underlyingActor.queries.keys.size shouldBe 1

      probe.send(writeCoordinatorActor,
                 MapInput(System.currentTimeMillis, db, namespace, "testMetric", testRecordSatisfy))

      val expectedAdd = awaitAssert {
        probe.expectMsgType[InputMapped]
      }
      expectedAdd.metric shouldBe "testMetric"
      expectedAdd.record shouldBe testRecordSatisfy

      subscriber.underlyingActor.receivedMessages shouldBe 1

      expectNoMessage(interval)
    }

    "delete a namespace" in within(5.seconds) {
      probe.send(writeCoordinatorActor, DeleteNamespace(db, namespace))

      awaitAssert {
        probe.expectMsgType[NamespaceDeleted]
      }

      Thread.sleep(2000)

      namespaceSchemaActor.underlyingActor.schemaActors.keys.size shouldBe 0
      metricsDataActor.underlyingActor.context.children.size shouldBe 0

      probe.send(metricsDataActor, GetNamespaces(db))

      val result = awaitAssert {
        probe.expectMsgType[NamespacesGot]
      }

      result.namespaces.size shouldBe 0
    }

    "delete entries" in within(5.seconds) {

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

    "drop a metric" in within(5.seconds) {
      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record1))
      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record2))

      probe.expectMsgType[InputMapped]
      probe.expectMsgType[InputMapped]

      expectNoMessage(interval)
      expectNoMessage(interval)

      probe.send(namespaceSchemaActor, GetSchema(db, namespace, "testMetric"))
      probe.expectMsgType[SchemaGot].schema.isDefined shouldBe true

      probe.send(metricsDataActor, GetCount(db, namespace, "testMetric"))

      probe.expectMsgType[CountGot].count shouldBe 2

      probe.send(writeCoordinatorActor, DropMetric(db, namespace, "testMetric"))
      awaitAssert {
        probe.expectMsgType[MetricDropped]
      }

      expectNoMessage(interval)

      probe.send(metricsDataActor, GetCount(db, namespace, "testMetric"))
      val result = awaitAssert {
        probe.expectMsgType[CountGot]
      }
      result.count shouldBe 0

      probe.send(namespaceSchemaActor, GetSchema(db, namespace, "testMetric"))
      probe.expectMsgType[SchemaGot].schema.isDefined shouldBe false
    }

  }
}
