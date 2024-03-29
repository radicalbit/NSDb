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
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsAdded
import io.radicalbit.nsdb.cluster.coordinator.mockedActors.{LocalMetadataCache, LocalMetadataCoordinator}
import io.radicalbit.nsdb.cluster.coordinator.mockedData.MockedData._
import io.radicalbit.nsdb.cluster.logic.LocalityReadNodesSelection
import io.radicalbit.nsdb.common.protocol.NSDbNode
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  MetricDropped,
  MetricsDataActorSubscribed,
  RecordAccumulated,
  SchemaUpdated
}
import io.radicalbit.nsdb.test.{NSDbIndexSpecLike, NSDbSpecLike}
import org.scalatest._

abstract class AbstractReadCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "ReadCoordinatorSpec",
        ConfigFactory
          .load()
          .withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("local"))
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
          .withValue("akka.log-dead-letters-during-shutdown", ConfigValueFactory.fromAnyRef("off"))
      ))
    with ImplicitSender
    with NSDbSpecLike
    with NSDbIndexSpecLike
    with BeforeAndAfterAll
    with WriteInterval {

  val probe     = TestProbe()
  val db        = "db"
  val namespace = "registry"

  val node               = NSDbNode("localhost", "node1", "volatile")
  val readNodesSelection = new LocalityReadNodesSelection("notImportant")

  val schemaCoordinator =
    system.actorOf(SchemaCoordinator.props(system.actorOf(Props[FakeSchemaCache])), "schema-coordinator")
  val metadataCoordinator =
    system.actorOf(LocalMetadataCoordinator.props(system.actorOf(Props[LocalMetadataCache])), "metadata-coordinator")
  val metricsDataActor =
    system.actorOf(MetricsDataActor.props(basePath.toString, node, Actor.noSender))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(metadataCoordinator,
                                                                  schemaCoordinator,
                                                                  system.actorOf(Props.empty),
                                                                  readNodesSelection)

  def prepareTestData() = {
    val location1 = Location(_: String, node, 0, 5)
    val location2 = Location(_: String, node, 6, 10)

    //long metric
    probe.send(metricsDataActor,
               DropMetricWithLocations(db,
                                       namespace,
                                       LongMetric.name,
                                       Seq(location1(LongMetric.name), location2(LongMetric.name))))
    probe.expectMsgType[MetricDropped]

    probe.send(schemaCoordinator, UpdateSchemaFromRecord(db, namespace, LongMetric.name, LongMetric.testRecords.head))
    probe.expectMsgType[SchemaUpdated]

    probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(location1(LongMetric.name))))
    probe.expectMsgType[LocationsAdded]

    LongMetric.recordsShard1.foreach(r => {
      probe.send(metricsDataActor, AddRecordToShard(db, namespace, location1(LongMetric.name), r))
      probe.expectMsgType[RecordAccumulated]
    })

    probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(location2(LongMetric.name))))
    probe.expectMsgType[LocationsAdded]

    LongMetric.recordsShard2.foreach(r => {
      probe.send(metricsDataActor, AddRecordToShard(db, namespace, location2(LongMetric.name), r))
      probe.expectMsgType[RecordAccumulated]
    })

    //double metric
    probe.send(metricsDataActor,
               DropMetricWithLocations(db,
                                       namespace,
                                       DoubleMetric.name,
                                       Seq(location1(DoubleMetric.name), location2(DoubleMetric.name))))
    probe.expectMsgType[MetricDropped]

    probe.send(schemaCoordinator,
               UpdateSchemaFromRecord(db, namespace, DoubleMetric.name, DoubleMetric.testRecords.head))
    probe.expectMsgType[SchemaUpdated]

    probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(location1(DoubleMetric.name))))
    probe.expectMsgType[LocationsAdded]
    DoubleMetric.recordsShard1.foreach(r => {
      probe.send(metricsDataActor, AddRecordToShard(db, namespace, location1(DoubleMetric.name), r))
      probe.expectMsgType[RecordAccumulated]
    })

    probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(location2(DoubleMetric.name))))
    probe.expectMsgType[LocationsAdded]
    DoubleMetric.recordsShard2.foreach(r => {
      probe.send(metricsDataActor, AddRecordToShard(db, namespace, location2(DoubleMetric.name), r))
      probe.expectMsgType[RecordAccumulated]
    })

    //aggregation long metric
    probe.send(
      metricsDataActor,
      DropMetricWithLocations(db,
                              namespace,
                              AggregationLongMetric.name,
                              Seq(location1(AggregationLongMetric.name), location2(AggregationLongMetric.name)))
    )
    probe.expectMsgType[MetricDropped]

    probe.send(
      schemaCoordinator,
      UpdateSchemaFromRecord(db, namespace, AggregationLongMetric.name, AggregationLongMetric.testRecords.head))
    probe.expectMsgType[SchemaUpdated]

    probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(location1(AggregationLongMetric.name))))
    probe.expectMsgType[LocationsAdded]
    AggregationLongMetric.recordsShard1.foreach(r => {
      probe.send(metricsDataActor, AddRecordToShard(db, namespace, location1(AggregationLongMetric.name), r))
      probe.expectMsgType[RecordAccumulated]
    })

    probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(location2(AggregationLongMetric.name))))
    probe.expectMsgType[LocationsAdded]
    AggregationLongMetric.recordsShard2.foreach(r => {
      probe.send(metricsDataActor, AddRecordToShard(db, namespace, location2(AggregationLongMetric.name), r))
      probe.expectMsgType[RecordAccumulated]
    })

    //aggregation double metric
    probe.send(
      metricsDataActor,
      DropMetricWithLocations(db,
                              namespace,
                              AggregationDoubleMetric.name,
                              Seq(location1(AggregationDoubleMetric.name), location2(AggregationDoubleMetric.name)))
    )
    probe.expectMsgType[MetricDropped]

    probe.send(
      schemaCoordinator,
      UpdateSchemaFromRecord(db, namespace, AggregationDoubleMetric.name, AggregationDoubleMetric.testRecords.head))
    probe.expectMsgType[SchemaUpdated]

    probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(location1(AggregationDoubleMetric.name))))
    probe.expectMsgType[LocationsAdded]
    AggregationDoubleMetric.recordsShard1.foreach(r => {
      probe.send(metricsDataActor, AddRecordToShard(db, namespace, location1(AggregationDoubleMetric.name), r))
      probe.expectMsgType[RecordAccumulated]
    })

    probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(location2(AggregationDoubleMetric.name))))
    probe.expectMsgType[LocationsAdded]
    AggregationDoubleMetric.recordsShard2.foreach(r => {
      probe.send(metricsDataActor, AddRecordToShard(db, namespace, location2(AggregationDoubleMetric.name), r))
      probe.expectMsgType[RecordAccumulated]
    })

  }

  override def beforeAll = {

    probe.send(readCoordinatorActor, SubscribeMetricsDataActor(metricsDataActor, node.uniqueNodeId))
    probe.expectMsgType[MetricsDataActorSubscribed]

    prepareTestData()

    expectNoMessage(indexingInterval)
  }

}
