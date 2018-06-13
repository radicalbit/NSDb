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
import io.radicalbit.nsdb.actors.SchemaActor
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.AddRecordToLocation
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import org.scalatest._

import scala.concurrent.Await

class ReadCoordinatorSpec
    extends TestKit(ActorSystem("ReadCoordinatorSpec"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with WriteInterval
    with ReadCoordinatorBehaviour {

  override val probe       = TestProbe()
  val probeActor           = probe.ref
  override val basePath    = "target/test_index/ReadCoordinatorSpec"
  override val db          = "db"
  override val namespace   = "registry"
  val schemaActor          = system.actorOf(SchemaActor.props(basePath, db, namespace))
  val metricsDataActor     = system.actorOf(MetricsDataActor.props(basePath))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(null, schemaActor)

  override def beforeAll(): Unit = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(5 second)

    val location = Location(_: String, "testNode", 0, 0)

    Await.result(readCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, "testNode"), 10 seconds)

    //long metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, LongMetric.name), 10 seconds)
    Await.result(schemaActor ? UpdateSchemaFromRecord(db, namespace, LongMetric.name, LongMetric.testRecords.head),
                 10 seconds)

    LongMetric.testRecords.foreach { record =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, record, location(LongMetric.name)),
                   10 seconds)
    }

    //double metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, DoubleMetric.name), 10 seconds)
    Await.result(schemaActor ? UpdateSchemaFromRecord(db, namespace, DoubleMetric.name, DoubleMetric.testRecords.head),
                 10 seconds)

    DoubleMetric.testRecords.foreach { record =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, record, location(DoubleMetric.name)),
                   10 seconds)
    }

    //aggregation metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, AggregationMetric.name), 10 seconds)
    Await.result(
      schemaActor ? UpdateSchemaFromRecord(db, namespace, AggregationMetric.name, AggregationMetric.testRecords.head),
      10 seconds)

    AggregationMetric.testRecords.foreach { record =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, record, location(AggregationMetric.name)),
                   10 seconds)
    }

    expectNoMessage(interval)
  }

  "ReadCoordinator" should behave.like(defaultBehaviour)
}
