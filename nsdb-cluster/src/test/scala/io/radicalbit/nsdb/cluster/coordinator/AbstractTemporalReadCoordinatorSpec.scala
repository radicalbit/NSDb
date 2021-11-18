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

import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsAdded
import io.radicalbit.nsdb.cluster.coordinator.mockedData.MockedData._
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{MetricDropped, RecordAccumulated, SchemaUpdated}

abstract class AbstractTemporalReadCoordinatorSpec extends AbstractReadCoordinatorSpec {

  override def prepareTestData(): Unit = {
    val location1 = Location(_: String, node, 100000, 190000)
    val location2 = Location(_: String, node, 0, 90000)

    //drop metrics
    probe.send(
      metricsDataActor,
      DropMetricWithLocations(db,
                              namespace,
                              TemporalLongMetric.name,
                              Seq(location1(TemporalLongMetric.name), location2(TemporalLongMetric.name)))
    )
    probe.expectMsgType[MetricDropped]
    probe.send(
      metricsDataActor,
      DropMetricWithLocations(db,
                              namespace,
                              TemporalDoubleMetric.name,
                              Seq(location1(TemporalDoubleMetric.name), location2(TemporalDoubleMetric.name)))
    )
    probe.expectMsgType[MetricDropped]

    probe.send(schemaCoordinator,
               UpdateSchemaFromRecord(db, namespace, TemporalLongMetric.name, TemporalLongMetric.testRecords.head))
    probe.expectMsgType[SchemaUpdated]
    probe.send(schemaCoordinator,
               UpdateSchemaFromRecord(db, namespace, TemporalDoubleMetric.name, TemporalDoubleMetric.testRecords.head))
    probe.expectMsgType[SchemaUpdated]

    probe.send(
      metadataCoordinator,
      AddLocations(db, namespace, Seq(location1(TemporalLongMetric.name), location1(TemporalDoubleMetric.name))))
    probe.expectMsgType[LocationsAdded]

    TemporalLongMetric.recordsShard1
      .foreach(r => {
        probe.send(metricsDataActor, AddRecordToShard(db, namespace, location1(TemporalLongMetric.name), r))
        probe.expectMsgType[RecordAccumulated]
      })
    TemporalDoubleMetric.recordsShard1
      .foreach(r => {
        probe.send(metricsDataActor, AddRecordToShard(db, namespace, location1(TemporalDoubleMetric.name), r))
        probe.expectMsgType[RecordAccumulated]
      })

    probe.send(
      metadataCoordinator,
      AddLocations(db, namespace, Seq(location2(TemporalLongMetric.name), location2(TemporalDoubleMetric.name))))
    probe.expectMsgType[LocationsAdded]

    TemporalLongMetric.recordsShard2
      .foreach(r => {
        probe.send(metricsDataActor, AddRecordToShard(db, namespace, location2(TemporalLongMetric.name), r))
        probe.expectMsgType[RecordAccumulated]
      })
    TemporalDoubleMetric.recordsShard2
      .foreach(r => {
        probe.send(metricsDataActor, AddRecordToShard(db, namespace, location2(TemporalDoubleMetric.name), r))
        probe.expectMsgType[RecordAccumulated]
      })
  }

}
