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
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import org.scalatest._

import scala.concurrent.Await

object LongMetric {

  val name = "longMetric"

  val recordsShard1: Seq[Bit] = Seq(
    Bit(1L, 1L, Map("surname" -> "Doe"), Map("name" -> "John")),
    Bit(2L, 2L, Map("surname" -> "Doe"), Map("name" -> "John")),
    Bit(4L, 3L, Map("surname" -> "D"), Map("name"   -> "J"))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(6L, 4L, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(8L, 5L, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(10L, 6L, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
  )

  val testRecords = recordsShard1 ++ recordsShard2
}

object DoubleMetric {

  val name = "doubleMetric"

  val recordsShard1: Seq[Bit] = Seq(
    Bit(2L, 1.5, Map("surname" -> "Doe"), Map("name" -> "John")),
    Bit(4L, 1.5, Map("surname" -> "Doe"), Map("name" -> "John"))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(6L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(8L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(10L, 1.5, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
  )

  val testRecords = recordsShard1 ++ recordsShard2
}

object AggregationMetric {

  val name = "aggregationMetric"

  val recordsShard1: Seq[Bit] = Seq(
    Bit(2L, 2L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 15L, "height" -> 30.5)),
    Bit(4L, 3L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 20L, "height" -> 30.5))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(6L, 5L, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
    Bit(8L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
    Bit(10L, 4L, Map("surname" -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
  )

  val testRecords = recordsShard1 ++ recordsShard2
}

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
  val basePath  = "target/test_index/ReadCoordinatorShardSpec"
  val db        = "db"
  val namespace = "registry"
  val schemaCoordinator =
    system.actorOf(SchemaCoordinator.props(system.actorOf(Props[FakeSchemaCache])), "schema-coordinator")
  val metadataCoordinator =
    system.actorOf(LocalMetadataCoordinator.props(system.actorOf(Props[LocalMetadataCache])), "metadata-coordinator")
  val metricsDataActor =
    system.actorOf(MetricsDataActor.props(basePath, "node1", Actor.noSender))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(metadataCoordinator,
                                                                  schemaCoordinator,
                                                                  system.actorOf(Props.empty))

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

    //aggregation metric
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 AggregationMetric.name,
                                                 Seq(location1(AggregationMetric.name),
                                                     location2(AggregationMetric.name))),
      10 seconds
    )

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
                                                            namespace,
                                                            AggregationMetric.name,
                                                            AggregationMetric.testRecords.head),
                 10 seconds)

    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location1(AggregationMetric.name))), 10 seconds)
    AggregationMetric.recordsShard1.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(AggregationMetric.name)),
                   10 seconds)
    })
    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location2(AggregationMetric.name))), 10 seconds)
    AggregationMetric.recordsShard2.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(AggregationMetric.name)),
                   10 seconds)
    })

    expectNoMessage(indexingInterval)
    expectNoMessage(indexingInterval)
  }
}
