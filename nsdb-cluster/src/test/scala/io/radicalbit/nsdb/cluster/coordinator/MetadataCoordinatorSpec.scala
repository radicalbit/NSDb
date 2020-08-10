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

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import akka.pattern._
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.cluster.actor.ClusterListener
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.ExecuteDeleteStatementInternalInLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.coordinator.mockedActors.{FakeCommitLogCoordinator, LocalMetadataCache}
import io.radicalbit.nsdb.cluster.logic.CapacityWriteNodesSelectionLogic
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.MetricInfoGot
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class MetadataCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "MetadataCoordinatorSpec",
        ConfigFactory
          .load()
          .withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
          .withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(2650))
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("60s"))
      ))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  import io.radicalbit.nsdb.cluster.coordinator.mockedActors.LocalMetadataCache._

  val writeNodesSelection = new CapacityWriteNodesSelectionLogic(
    CapacityWriteNodesSelectionLogic.fromConfigValue(system.settings.config.getString("nsdb.cluster.metrics-selector")))

  val probe                = TestProbe()
  val commitLogCoordinator = system.actorOf(Props[FakeCommitLogCoordinator])
  val schemaCache          = system.actorOf(Props[FakeSchemaCache])
  val schemaCoordinator =
    system.actorOf(SchemaCoordinator.props(schemaCache), "schemacoordinator")
  val metricsDataActorProbe = TestProbe()
  val metadataCache         = system.actorOf(Props[LocalMetadataCache])
  val clusterListener       = system.actorOf(Props[ClusterListener])

  val metadataCoordinator =
    system.actorOf(
      MetadataCoordinator.props(clusterListener, metadataCache, schemaCache, probe.ref, writeNodesSelection))

  val db        = "testDb"
  val namespace = "testNamespace"
  val metric    = "testMetric"

  implicit val timeout = Timeout(10 seconds)

  override def beforeAll = {
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)

    Await.result(metadataCoordinator ? SubscribeCommitLogCoordinator(commitLogCoordinator, "localhost"), 10 seconds)
    Await.result(metadataCoordinator ? SubscribeMetricsDataActor(metricsDataActorProbe.ref, "localhost"), 10 seconds)

    val nameRecord = Bit(0, 1, Map("name" -> "name"), Map("city" -> "milano"))
    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, metric, nameRecord), 10 seconds)
  }

  override def beforeEach: Unit = {
    implicit val timeout = Timeout(5 seconds)
    Await.result(metadataCache ? DeleteAll, 5 seconds)
  }

  "MetadataCoordinator" should {
    "add multiple locations for a metric" in {

      val locations = Seq(Location(metric, "node_01", 0L, 30000L),
                          Location(metric, "node_01", 0L, 60000L),
                          Location(metric, "node_01", 0L, 90000L))

      probe.send(metadataCoordinator, AddLocations(db, namespace, locations))
      val locationAdded = awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      locationAdded.db shouldBe db
      locationAdded.namespace shouldBe namespace
      locationAdded.locations.sortBy(_.to) shouldBe locations

      probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
      awaitAssert {
        probe.expectMsgType[LocationsGot].locations.sortBy(_.to) shouldBe locations
      }
    }

    "retrieve a Location for a metric" in {
      probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(Location(metric, "node_01", 0L, 30000L))))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(Location(metric, "node_02", 0L, 30000L))))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
      val retrievedLocations = awaitAssert {
        probe.expectMsgType[LocationsGot]
      }

      retrievedLocations.locations.size shouldBe 2
      val loc = retrievedLocations.locations.head
      loc.metric shouldBe metric
      loc.from shouldBe 0L
      loc.to shouldBe 30000L
      loc.node shouldBe "node_01"

      val loc2 = retrievedLocations.locations.last
      loc2.metric shouldBe metric
      loc2.from shouldBe 0L
      loc2.to shouldBe 30000L
      loc2.node shouldBe "node_02"
    }

    "retrieve Locations for a metric" in {

      val loc11 = Location(metric, "node_01", 0L, 30000L)
      val loc21 = Location(metric, "node_02", 0L, 30000L)
      val loc12 = Location(metric, "node_01", 30000L, 60000L)
      val loc22 = Location(metric, "node_02", 30000L, 60000L)

      probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(loc11)))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(loc21)))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(loc12)))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(loc22)))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
      val retrievedLocations = awaitAssert {
        probe.expectMsgType[LocationsGot]
      }

      retrievedLocations.locations.size shouldBe 4
      val locs = retrievedLocations.locations

      locs should contain(loc11)
      locs should contain(loc21)
      locs should contain(loc12)
      locs should contain(loc22)
    }

    "retrieve correct default write Location given a timestamp" in {

      awaitAssert {
        probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 1))
        val locationGot = probe.expectMsgType[WriteLocationsGot]
        locationGot.db shouldBe db
        locationGot.namespace shouldBe namespace
        locationGot.metric shouldBe metric

        locationGot.locations.size shouldBe 1
        locationGot.locations.head.from shouldBe 0L
        locationGot.locations.head.to shouldBe 60000L
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 60001))
        val locationGot_2 = probe.expectMsgType[WriteLocationsGot]
        locationGot_2.db shouldBe db
        locationGot_2.namespace shouldBe namespace
        locationGot_2.metric shouldBe metric

        locationGot_2.locations.size shouldBe 1
        locationGot_2.locations.head.from shouldBe 60000L
        locationGot_2.locations.head.to shouldBe 120000L
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 60002))
        val locationGot_3 = probe.expectMsgType[WriteLocationsGot]
        locationGot_3.db shouldBe db
        locationGot_3.namespace shouldBe namespace
        locationGot_3.metric shouldBe metric

        locationGot_3.locations.size shouldBe 1
        locationGot_3.locations.head.from shouldBe 60000L
        locationGot_3.locations.head.to shouldBe 120000L
      }

    }

    "retrieve correct write Location for a initialized metric with a different shard interval" in {

      val metricInfo = MetricInfo(db, namespace, metric, 100)

      probe.send(metadataCoordinator, PutMetricInfo(metricInfo))
      awaitAssert {
        probe.expectMsgType[MetricInfoPut]
      }.metricInfo shouldBe metricInfo

      probe.send(metadataCoordinator, GetMetricInfo(db, namespace, metric))
      awaitAssert {
        probe.expectMsgType[MetricInfoGot]
      }.metricInfo shouldBe Some(metricInfo)

      probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 1))
      val locationGot = awaitAssert {
        probe.expectMsgType[WriteLocationsGot]
      }

      locationGot.db shouldBe db
      locationGot.namespace shouldBe namespace
      locationGot.metric shouldBe metric
      locationGot.locations.size shouldBe 1

      locationGot.locations.head.from shouldBe 0L
      locationGot.locations.head.to shouldBe 100L

      probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 101))
      val locationGot_2 = awaitAssert {
        probe.expectMsgType[WriteLocationsGot]
      }

      locationGot_2.db shouldBe db
      locationGot_2.namespace shouldBe namespace
      locationGot_2.metric shouldBe metric

      locationGot_2.locations.size shouldBe 1
      locationGot_2.locations.head.from shouldBe 100L
      locationGot_2.locations.head.to shouldBe 200L

      probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 202))
      val locationGot_3 = awaitAssert {
        probe.expectMsgType[WriteLocationsGot]
      }

      locationGot_3.db shouldBe db
      locationGot_3.namespace shouldBe namespace
      locationGot_3.metric shouldBe metric

      locationGot_3.locations.size shouldBe 1
      locationGot_3.locations.head.from shouldBe 200L
      locationGot_3.locations.head.to shouldBe 300L
    }

    "retrieve metric infos" in {

      val metricInfo = MetricInfo(db, namespace, metric, 100)

      probe.send(metadataCoordinator, GetMetricInfo(db, namespace, metric))
      awaitAssert {
        probe.expectMsgType[MetricInfoGot]
      }.metricInfo.isEmpty shouldBe true

      probe.send(metadataCoordinator, PutMetricInfo(metricInfo))
      awaitAssert {
        probe.expectMsgType[MetricInfoPut]
      }.metricInfo shouldBe metricInfo

      probe.send(metadataCoordinator, GetMetricInfo(db, namespace, metric))
      awaitAssert {
        probe.expectMsgType[MetricInfoGot]
      }.metricInfo shouldBe Some(metricInfo)
    }

    "not allow to insert a metric info already inserted" in {

      val metricInfo = MetricInfo(db, namespace, metric, 100)

      probe.send(metadataCoordinator, PutMetricInfo(metricInfo))
      awaitAssert {
        probe.expectMsgType[MetricInfoPut]
      }.metricInfo shouldBe metricInfo

      probe.send(metadataCoordinator, GetMetricInfo(db, namespace, metric))
      awaitAssert {
        probe.expectMsgType[MetricInfoGot]
      }.metricInfo shouldBe Some(metricInfo)

      probe.send(metadataCoordinator, PutMetricInfo(metricInfo))
      awaitAssert {
        probe.expectMsgType[MetricInfoFailed]
      }
    }

    "evict outdated locations from cache" in {

      val metricInfo = MetricInfo(db, namespace, metric, 100, 5000)

      probe.send(metadataCoordinator, PutMetricInfo(metricInfo))
      awaitAssert {
        probe.expectMsgType[MetricInfoPut]
      }.metricInfo shouldBe metricInfo

      val now = System.currentTimeMillis()

      val locations = Seq(
        Location(metric, "localhost", 0L, 30000L),
        Location(metric, "localhost", 30000L, 60000L),
        Location(metric, "localhost", now - 5000, now - 2000),
        Location(metric, "localhost", now - 1000, now)
      )

      locations.foreach { loc =>
        probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(loc)))
        awaitAssert {
          probe.expectMsgType[LocationsAdded]
        }
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
        val locationsGot = probe.expectMsgType[LocationsGot]
        locationsGot.locations.sortBy(_.from) shouldBe locations
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
        val locationsGot = probe.expectMsgType[LocationsGot]
        locationsGot.locations.sortBy(_.from) shouldBe locations.takeRight(2)
      }

      awaitAssert {
        val deleteCommand = metricsDataActorProbe.expectMsgType[ExecuteDeleteStatementInternalInLocations]
        deleteCommand.locations shouldBe Seq(locations.takeRight(2).head)
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
        val locationsGot = probe.expectMsgType[LocationsGot]
        locationsGot.locations.sortBy(_.from) shouldBe locations.takeRight(1)

        val deleteCommand = metricsDataActorProbe.expectMsgType[ExecuteDeleteStatementInternalInLocations]
        deleteCommand.locations shouldBe locations.takeRight(1)
      }
    }

  }

}
