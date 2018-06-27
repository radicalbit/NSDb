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

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.actors.SchemaActor
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.AddRecordToLocation
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class ReadCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "ReadCoordinatorSpec",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
      ))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ReadCoordinatorBehaviour
    with WriteInterval {

  override val probe                = TestProbe()
  override val basePath             = "target/test_index/ReadCoordinatorSpec"
  override val db                   = "db"
  override val namespace            = "registry"
  val schemaActor                   = system.actorOf(SchemaActor.props(basePath, db, namespace))
  val metricsDataActor              = system.actorOf(MetricsDataActor.props(basePath))
  override val readCoordinatorActor = system actorOf ReadCoordinator.props(null, schemaActor)

  override def beforeAll = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(5 second)

    Await.result(readCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, "node1"), 10 seconds)

    val location1 = Location(_: String, "node1", 0, 5)
    val location2 = Location(_: String, "node1", 6, 10)

    //long metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, LongMetric.name), 10 seconds)

    Await.result(schemaActor ? UpdateSchemaFromRecord(db, namespace, LongMetric.name, LongMetric.testRecords.head),
                 10 seconds)

    LongMetric.recordsShard1.foreach(r =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(LongMetric.name)), 10 seconds))
    LongMetric.recordsShard2.foreach(r =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(LongMetric.name)), 10 seconds))

    //double metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, DoubleMetric.name), 10 seconds)

    Await.result(schemaActor ? UpdateSchemaFromRecord(db, namespace, DoubleMetric.name, DoubleMetric.testRecords.head),
                 10 seconds)

    DoubleMetric.recordsShard1.foreach(r =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(DoubleMetric.name)), 10 seconds))
    DoubleMetric.recordsShard2.foreach(r =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(DoubleMetric.name)), 10 seconds))

    //aggregation metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, AggregationMetric.name), 10 seconds)

    Await.result(
      schemaActor ? UpdateSchemaFromRecord(db, namespace, AggregationMetric.name, AggregationMetric.testRecords.head),
      10 seconds)

    AggregationMetric.recordsShard1.foreach(
      r =>
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(AggregationMetric.name)),
                     10 seconds))
    AggregationMetric.recordsShard2.foreach(
      r =>
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(AggregationMetric.name)),
                     10 seconds))

    expectNoMessage(interval)
    expectNoMessage(interval)
  }

  "ReadCoordinator" should behave.like(defaultBehaviour)

  "ReadCoordinator" when {

    "receive a select projecting a wildcard with a limit" should {
      "execute it successfully" in within(5.seconds) {

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = AllFields,
                               limit = Some(LimitOperator(2)))
          )
        )
        awaitAssert {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size shouldBe 2
        }
      }
    }

    "receive a select projecting a wildcard with a limit and a ordering" should {
      "execute it successfully when ordered by timestamp" in within(5.seconds) {

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = AllFields,
              limit = Some(LimitOperator(2)),
              order = Some(DescOrderOperator("timestamp"))
            )
          )
        )
        awaitAssert {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size shouldBe 2
          expected.values shouldBe LongMetric.recordsShard2.tail.reverse
        }
      }

      "execute it successfully when ordered by another dimension" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = AllFields,
                               limit = Some(LimitOperator(2)),
                               order = Some(DescOrderOperator("name")))
          )
        )
        awaitAssert {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size shouldBe 2
          LongMetric.recordsShard1 foreach { r =>
            expected.values.contains(r) shouldBe true
          }
        }
      }
    }

  }
}
