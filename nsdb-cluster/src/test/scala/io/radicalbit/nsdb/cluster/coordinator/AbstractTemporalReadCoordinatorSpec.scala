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

import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.AddRecordToLocation
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocations
import io.radicalbit.nsdb.cluster.coordinator.mockedData.MockedData._
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._

import scala.concurrent.Await

abstract class AbstractTemporalReadCoordinatorSpec extends AbstractReadCoordinatorSpec {

  override def beforeAll: Unit = {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5.second)

    Await.result(readCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, "node1"), 10 seconds)

    val location1 = Location(_: String, "node1", 100000, 190000)
    val location2 = Location(_: String, "node1", 0, 90000)

    //drop metrics
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
        namespace,
        TemporalLongMetric.name,
        Seq(location1(TemporalLongMetric.name),
          location2(TemporalLongMetric.name))),
      10 seconds
    )
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
        namespace,
        TemporalDoubleMetric.name,
        Seq(location1(TemporalDoubleMetric.name),
          location2(TemporalDoubleMetric.name))),
      10 seconds
    )

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
      namespace,
      TemporalLongMetric.name,
      TemporalLongMetric.testRecords.head),
      10 seconds)
    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
      namespace,
      TemporalDoubleMetric.name,
      TemporalDoubleMetric.testRecords.head),
      10 seconds)

    Await.result(
      metadataCoordinator ? AddLocations(db,
        namespace,
        Seq(location1(TemporalLongMetric.name), location1(TemporalDoubleMetric.name))),
      10 seconds)

    TemporalLongMetric.recordsShard1
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(TemporalLongMetric.name)),
          10 seconds)
      })
    TemporalDoubleMetric.recordsShard1
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(TemporalDoubleMetric.name)),
          10 seconds)
      })

    Await.result(
      metadataCoordinator ? AddLocations(db,
        namespace,
        Seq(location2(TemporalLongMetric.name), location2(TemporalDoubleMetric.name))),
      10 seconds)

    TemporalLongMetric.recordsShard2
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(TemporalLongMetric.name)),
          10 seconds)
      })
    TemporalDoubleMetric.recordsShard2
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(TemporalDoubleMetric.name)),
          10 seconds)
      })

    expectNoMessage(indexingInterval)
  }
}
