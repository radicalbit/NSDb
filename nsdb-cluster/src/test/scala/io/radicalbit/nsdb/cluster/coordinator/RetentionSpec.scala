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

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.cluster.`extension`.NSDbClusterSnapshot
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache.{
  AllMetricInfoWithRetentionGot,
  GetAllMetricInfoWithRetention
}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.PutMetricInfo
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.MetricInfoPut
import io.radicalbit.nsdb.cluster.coordinator.mockedActors.{
  FakeCommitLogCoordinator,
  LocalMetadataCache,
  MockedClusterListener
}
import io.radicalbit.nsdb.cluster.logic.{CapacityWriteNodesSelectionLogic, LocalityReadNodesSelection}
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement.{AscOrderOperator, ListFields, SelectSQLStatement}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{InputMapped, SelectStatementExecuted}
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class RetentionSpec
    extends TestKit(
      ActorSystem(
        "RetentionSpec",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
          .withValue("nsdb.write.scheduler.interval", ConfigValueFactory.fromAnyRef("5ms"))
          .withValue("nsdb.retention.check.interval", ConfigValueFactory.fromAnyRef("20ms"))
      ))
    with ImplicitSender
    with NSDbSpecLike
    with BeforeAndAfterAll
    with WriteInterval {

  val probe     = TestProbe()
  val basePath  = "target/test_index/RetentionSpec"
  val db        = "db"
  val namespace = "namespace"

  val metricWithRetention    = "metricWithRetention"
  val metricWithoutRetention = "metricWithoutRetention"

  val records: Seq[Bit] = Seq(
    Bit(1L, 1L, Map("surname"  -> "Doe"), Map("name" -> "John")),
    Bit(2L, 2L, Map("surname"  -> "Doe"), Map("name" -> "John")),
    Bit(4L, 3L, Map("surname"  -> "D"), Map("name"   -> "J")),
    Bit(6L, 4L, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(8L, 5L, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(10L, 6L, Map("surname" -> "Doe"), Map("name" -> "Frankie")),
    Bit(12L, 7L, Map("surname" -> "Doe"), Map("name" -> "Bill")),
    Bit(14L, 8L, Map("surname" -> "Doe"), Map("name" -> "Frank")),
    Bit(16L, 9L, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
  )

  val writeNodesSelection = new CapacityWriteNodesSelectionLogic(
    CapacityWriteNodesSelectionLogic.fromConfigValue(system.settings.config.getString("nsdb.cluster.metrics-selector")))
  val readSelectionLogic = new LocalityReadNodesSelection("notImportant")

  val commitLogCoordinator = system.actorOf(Props[FakeCommitLogCoordinator])
  val schemaCache          = system.actorOf(Props[FakeSchemaCache])
  val schemaCoordinator    = system.actorOf(SchemaCoordinator.props(schemaCache), "schema-coordinator")
  val localMetadataCache   = system.actorOf(Props[LocalMetadataCache])
  val metadataCoordinator =
    system.actorOf(
      MetadataCoordinator
        .props(system.actorOf(Props[MockedClusterListener]),
               localMetadataCache,
               schemaCache,
               system.actorOf(Props.empty),
               writeNodesSelection)
        .withDispatcher("akka.actor.control-aware-dispatcher"),
      "metadata-coordinator"
    )
  val writeCoordinator =
    system.actorOf(WriteCoordinator.props(metadataCoordinator, schemaCoordinator, system.actorOf(Props.empty)),
                   "write-coordinator")
  val readCoordinatorActor = system actorOf ReadCoordinator.props(metadataCoordinator,
                                                                  schemaCoordinator,
                                                                  system.actorOf(Props.empty),
                                                                  readSelectionLogic)

  implicit val timeout = Timeout(5.second)

  private def selectAllOrderByTimestamp(metric: String) = ExecuteStatement(
    SelectSQLStatement(
      db = db,
      namespace = namespace,
      metric = metric,
      distinct = false,
      fields = ListFields(List.empty),
      condition = None,
      groupBy = None,
      order = Some(AscOrderOperator("timestamp"))
    )
  )

  override def beforeAll = {
    val node = NSDbNode("localhost", "nodeId", "volatile")
    NSDbClusterSnapshot(system).addNode(node)

    awaitAssert {
      val nodes = NSDbClusterSnapshot(system).nodes
      nodes.size shouldBe 1
    }

    val metricsDataActor =
      system.actorOf(MetricsDataActor.props(basePath, node, Actor.noSender))

    Await.result(readCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, node.uniqueNodeId), 10 seconds)
    Await.result(metadataCoordinator ? SubscribeMetricsDataActor(metricsDataActor, node.uniqueNodeId), 10 seconds)
    Await.result(metadataCoordinator ? SubscribeCommitLogCoordinator(commitLogCoordinator, node.uniqueNodeId),
                 10 seconds)
    Await.result(writeCoordinator ? SubscribeMetricsDataActor(metricsDataActor, node.uniqueNodeId), 10 seconds)
    Await.result(writeCoordinator ? SubscribeCommitLogCoordinator(commitLogCoordinator, node.uniqueNodeId), 10 seconds)

    Await.result(writeCoordinator ? DropMetric(db, namespace, metricWithRetention), 10 seconds)
    Await.result(writeCoordinator ? DropMetric(db, namespace, metricWithoutRetention), 10 seconds)

    records.foreach { r =>
      Await.result(writeCoordinator ? MapInput(r.timestamp, db, namespace, metricWithoutRetention, r), 10 seconds) shouldBe a[
        InputMapped]
    }

    expectNoMessage(indexingInterval)
  }

  "NSDb Retention" ignore {

    "is not set to a metric" should {

      "do nothing" in {
        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithoutRetention)
          )
          probe.expectMsgType[SelectStatementExecuted].values.size should be(9)
        }

        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithoutRetention)
          )
          probe.expectMsgType[SelectStatementExecuted].values.size should be(9)
        }
      }

    }

    "is set to a metric" should {

      "delete outdated records" in {

        def currentRecords(currentTime: Long, retention: Long): Seq[Bit] = Seq(
          Bit(currentTime + retention - 3000, 1L, Map("surname" -> "Doe"), Map("name" -> "John")),
          Bit(currentTime + retention - 1000, 2L, Map("surname" -> "Doe"), Map("name" -> "John")),
          Bit(currentTime + retention - 700, 3L, Map("surname"  -> "D"), Map("name"   -> "J")),
          Bit(currentTime + retention - 500, 4L, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
          Bit(currentTime + retention - 300, 5L, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
          Bit(currentTime + retention - 200, 6L, Map("surname"  -> "Doe"), Map("name" -> "Frankie")),
          Bit(currentTime + retention - 100, 7L, Map("surname"  -> "Doe"), Map("name" -> "Bill"))
        )

        val retention = 2000

        val retentionMetricInfo = MetricInfo(db, namespace, metricWithRetention, 5000, retention)

        probe.send(metadataCoordinator, PutMetricInfo(retentionMetricInfo))

        awaitAssert {
          probe.expectMsg(MetricInfoPut(retentionMetricInfo))
        }

        awaitAssert {
          probe.send(localMetadataCache, GetAllMetricInfoWithRetention)
          probe.expectMsgType[AllMetricInfoWithRetentionGot].metricInfo shouldBe Set(retentionMetricInfo)
        }

        val currentTime = System.currentTimeMillis()

        val recordsToTest = currentRecords(currentTime, retention)

        recordsToTest.foreach { r =>
          probe.send(writeCoordinator, MapInput(r.timestamp, db, namespace, metricWithRetention, r))
          probe.expectMsgType[InputMapped]
        }

        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithRetention)
          )
          val result = probe.expectMsgType[SelectStatementExecuted]
          result.values.size shouldBe 7
          result.values shouldBe recordsToTest.takeRight(7)
        }

        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithRetention)
          )
          val result = probe.expectMsgType[SelectStatementExecuted]
          result.values.size shouldBe 6
          result.values shouldBe recordsToTest.takeRight(6)
        }

        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithRetention)
          )
          val result = probe.expectMsgType[SelectStatementExecuted]
          result.values.size shouldBe 5
          result.values shouldBe recordsToTest.takeRight(5)
        }

        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithRetention)
          )
          val result = probe.expectMsgType[SelectStatementExecuted].values
          result.size shouldBe 4
          result shouldBe recordsToTest.takeRight(4)
        }

        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithRetention)
          )
          val result = probe.expectMsgType[SelectStatementExecuted].values
          result.size shouldBe 3
          result shouldBe recordsToTest.takeRight(3)
        }

        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithRetention)
          )
          val result = probe.expectMsgType[SelectStatementExecuted].values
          result.size shouldBe 2
          result shouldBe recordsToTest.takeRight(2)
        }

        awaitAssert {
          probe.send(
            readCoordinatorActor,
            selectAllOrderByTimestamp(metricWithRetention)
          )
          val result = probe.expectMsgType[SelectStatementExecuted].values
          result shouldBe Seq()
        }

      }

    }

  }

}
