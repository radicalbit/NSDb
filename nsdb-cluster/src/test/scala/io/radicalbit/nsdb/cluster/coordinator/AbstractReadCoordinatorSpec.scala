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
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.AddRecordToLocation
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocations
import io.radicalbit.nsdb.cluster.coordinator.mockedActors.{LocalMetadataCache, LocalMetadataCoordinator}
import io.radicalbit.nsdb.cluster.coordinator.mockedData.MockedData._
import io.radicalbit.nsdb.cluster.logic.LocalityReadNodesSelection
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import org.scalatest._

import scala.concurrent.Await

abstract class AbstractReadCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "ReadCoordinatorSpec",
        ConfigFactory
          .load()
          .withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("local"))
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
      ))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with WriteInterval {

  val probe     = TestProbe()
  val basePath  = s"target/test_index/${getClass.getName}"
  val db        = "db"
  val namespace = "registry"

  val readNodesSelection = new LocalityReadNodesSelection("notImportant")

  val schemaCoordinator =
    system.actorOf(SchemaCoordinator.props(system.actorOf(Props[FakeSchemaCache])), "schema-coordinator")
  val metadataCoordinator =
    system.actorOf(LocalMetadataCoordinator.props(system.actorOf(Props[LocalMetadataCache])), "metadata-coordinator")
  val metricsDataActor =
    system.actorOf(MetricsDataActor.props(basePath, "node1", Actor.noSender))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(metadataCoordinator,
                                                                  schemaCoordinator,
                                                                  system.actorOf(Props.empty),
                                                                  readNodesSelection)

  override def beforeAll = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(5.second)

    Await.result(readCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, "node1"), 10 seconds)

    val location1 = Location(_: String, "node1", 0, 5)
    val location2 = Location(_: String, "node1", 6, 10)

    //long metric
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 LongMetric.name,
                                                 Seq(location1(LongMetric.name), location2(LongMetric.name))),
      10 seconds)

    Await.result(
      schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, LongMetric.name, LongMetric.testRecords.head),
      10 seconds)

    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location1(LongMetric.name))), 10 seconds)
    LongMetric.recordsShard1.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(LongMetric.name)), 10 seconds)
    })
    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location2(LongMetric.name))), 10 seconds)
    LongMetric.recordsShard2.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(LongMetric.name)), 10 seconds)
    })

    //double metric
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 DoubleMetric.name,
                                                 Seq(location1(DoubleMetric.name), location2(DoubleMetric.name))),
      10 seconds)

    Await.result(
      schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, DoubleMetric.name, DoubleMetric.testRecords.head),
      10 seconds)

    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location1(DoubleMetric.name))), 10 seconds)
    DoubleMetric.recordsShard1.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(DoubleMetric.name)), 10 seconds)
    })
    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location2(DoubleMetric.name))), 10 seconds)
    DoubleMetric.recordsShard2.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(DoubleMetric.name)), 10 seconds)
    })

    //aggregation long metric
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 AggregationLongMetric.name,
                                                 Seq(location1(AggregationLongMetric.name),
                                                     location2(AggregationLongMetric.name))),
      10 seconds
    )

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
                                                            namespace,
                                                            AggregationLongMetric.name,
                                                            AggregationLongMetric.testRecords.head),
                 10 seconds)

    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location1(AggregationLongMetric.name))),
                 10 seconds)
    AggregationLongMetric.recordsShard1.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(AggregationLongMetric.name)),
                   10 seconds)
    })
    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location2(AggregationLongMetric.name))),
                 10 seconds)
    AggregationLongMetric.recordsShard2.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(AggregationLongMetric.name)),
                   10 seconds)
    })

    //aggregation double metric
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 AggregationDoubleMetric.name,
                                                 Seq(location1(AggregationDoubleMetric.name),
                                                     location2(AggregationDoubleMetric.name))),
      10 seconds
    )

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
                                                            namespace,
                                                            AggregationDoubleMetric.name,
                                                            AggregationDoubleMetric.testRecords.head),
                 10 seconds)

    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location1(AggregationDoubleMetric.name))),
                 10 seconds)
    AggregationDoubleMetric.recordsShard1.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(AggregationDoubleMetric.name)),
                   10 seconds)
    })
    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location2(AggregationDoubleMetric.name))),
                 10 seconds)
    AggregationDoubleMetric.recordsShard2.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(AggregationDoubleMetric.name)),
                   10 seconds)
    })

    expectNoMessage(indexingInterval)
    expectNoMessage(indexingInterval)
  }
}
