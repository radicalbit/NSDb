/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import akka.actor.{Actor, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.PublisherActor.Commands.SubscribeBySqlStatement
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor
import io.radicalbit.nsdb.cluster.coordinator.mockedActors.{
  FakeCommitLogCoordinator,
  LocalMetadataCache,
  MockedClusterListener
}
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.cluster.logic.CapacityWriteNodesSelectionLogic
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbNode}
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.protocol.RealTimeProtocol.Events.{RecordsPublished, SubscribedByQueryString}
import io.radicalbit.nsdb.test.NSDbSpecLike

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

class TestSubscriber extends Actor {
  var receivedMessages = 0
  def receive: Receive = {
    case RecordsPublished(_, _, _, _, _) =>
      receivedMessages += 1
  }
}

class FakeReadCoordinatorActor extends Actor {
  def receive: Receive = {
    case ExecuteStatement(statement, _) =>
      sender() ! SelectStatementExecuted(statement, values = Seq.empty, Schema(statement.metric, Bit.empty))
  }
}

trait WriteCoordinatorBehaviour { this: TestKit with NSDbSpecLike =>

  val probe = TestProbe()

  def basePath: String

  def db: String

  def namespace: String

  val node: NSDbNode = NSDbNode("localhost", "nodeId", "volatile")

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + 1.second

  val writeNodesSelection = new CapacityWriteNodesSelectionLogic(
    CapacityWriteNodesSelectionLogic.fromConfigValue(system.settings.config.getString("nsdb.cluster.metrics-selector")))

  lazy val commitLogCoordinator = system.actorOf(Props[FakeCommitLogCoordinator], "commitLogCoordinator")
  lazy val schemaCache          = system.actorOf(Props[FakeSchemaCache], "schemaCache")
  lazy val schemaCoordinator    = system.actorOf(SchemaCoordinator.props(schemaCache), "schemaCoordinator")
  lazy val subscriber           = TestActorRef[TestSubscriber](Props[TestSubscriber], "testSubscriber")
  lazy val publisherActor =
    TestActorRef[PublisherActor](PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor])),
                                 "publisherActor")
  lazy val metadataCoordinator = system.actorOf(
    MetadataCoordinator.props(system.actorOf(Props[MockedClusterListener]),
                              system.actorOf(Props[LocalMetadataCache]),
                              schemaCache,
                              system.actorOf(Props.empty),
                              writeNodesSelection),
    "metadataCoordinator"
  )
  lazy val writeCoordinatorActor = system.actorOf(
    WriteCoordinator.props(metadataCoordinator, schemaCoordinator, system.actorOf(Props.empty)),
    "writeCoordinator")
  lazy val metricsDataActor =
    TestActorRef[MetricsDataActor](MetricsDataActor.props(basePath, node, writeCoordinatorActor))

  val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"), Map.empty)
  val record2 =
    Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"), Map.empty)

  NSDbClusterSnapshot(system).addNode(node)

  def defaultBehaviour {
    "write records" in {
      val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"), Map.empty)
      val record2 =
        Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"), Map.empty)
      val incompatibleRecord =
        Bit(System.currentTimeMillis, 3, Map("content" -> 1, "content2" -> s"content2"), Map.empty)

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

    "write records and publish event to its subscriber" in {
      val testRecordSatisfy = Bit(100, 1, Map("name" -> "john"), Map.empty)

      val testSqlStatement = SelectSQLStatement(
        db = db,
        namespace = namespace,
        metric = "testMetric",
        distinct = false,
        fields = AllFields(),
        condition = Some(
          Condition(
            ComparisonExpression(dimension = "timestamp",
                                 comparison = GreaterOrEqualToOperator,
                                 value = AbsoluteComparisonValue(10L)))),
        limit = Some(LimitOperator(4))
      )

      probe.send(publisherActor,
                 SubscribeBySqlStatement(subscriber, db, namespace, "testMetric", "testQueryString", testSqlStatement))
      probe.expectMsgType[SubscribedByQueryString]
      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1
      publisherActor.underlyingActor.plainQueries.keys.size shouldBe 1

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

    "delete a namespace" in {
      probe.send(writeCoordinatorActor, DeleteNamespace(db, namespace))

      awaitAssert {
        probe.expectMsgType[NamespaceDeleted]
      }

      expectNoMessage(interval)

      metricsDataActor.underlyingActor.context.children.map(_.path.name).exists(_.contains(namespace)) shouldBe false

      probe.send(metadataCoordinator, GetNamespaces(db))

      val result = awaitAssert {
        probe.expectMsgType[NamespacesGot]
      }

      result.namespaces.exists(_.contains(namespace)) shouldBe false
    }

    "delete entries" in {

      val records: Seq[Bit] = Seq(
        Bit(2, 1, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis()), Map.empty),
        Bit(4, 1, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis()), Map.empty),
        Bit(6, 1, Map("name"  -> "Bill", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis()), Map.empty),
        Bit(8, 1, Map("name"  -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()), Map.empty),
        Bit(10, 1, Map("name" -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()), Map.empty)
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
            condition = Condition(
              RangeExpression(dimension = "timestamp",
                              value1 = AbsoluteComparisonValue(2L),
                              value2 = AbsoluteComparisonValue(4L)))
          )
        )
      )
      awaitAssert {
        probe.expectMsgType[DeleteStatementExecuted]
      }

    }

    "drop a metric" in {
      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record1))
      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record2))

      probe.expectMsgType[InputMapped]
      probe.expectMsgType[InputMapped]

      expectNoMessage(interval)
      expectNoMessage(interval)

      probe.send(schemaCoordinator, GetSchema(db, namespace, "testMetric"))
      probe.expectMsgType[SchemaGot].schema.isDefined shouldBe true

      probe.send(metadataCoordinator, GetLiveLocations(db, namespace, "testMetric"))
      val locations = probe.expectMsgType[LiveLocationsGot].locations

      probe.send(metricsDataActor, GetCountWithLocations(db, namespace, "testMetric", locations))

      probe.expectMsgType[CountGot].count shouldBe 2

      probe.send(writeCoordinatorActor, DropMetric(db, namespace, "testMetric"))
      awaitAssert {
        probe.expectMsgType[MetricDropped]
      }

      expectNoMessage(interval)

      probe.send(metadataCoordinator, GetLiveLocations(db, namespace, "testMetric"))
      val locationsAfterDrop = probe.expectMsgType[LiveLocationsGot].locations
      locationsAfterDrop.size shouldBe 0

      probe.send(metricsDataActor, GetCountWithLocations(db, namespace, "testMetric", locationsAfterDrop))
      val result = awaitAssert {
        probe.expectMsgType[CountGot]
      }
      result.count shouldBe 0

      probe.send(schemaCoordinator, GetSchema(db, namespace, "testMetric"))
      probe.expectMsgType[SchemaGot].schema.isDefined shouldBe false
    }

  }
}
