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
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.ExecuteDeleteStatementInternalInLocations
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache.{AddNodeToBlackList, NodeToBlackListAdded}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.coordinator.mockedActors.{
  FakeCommitLogCoordinator,
  LocalMetadataCache,
  MockedClusterListener
}
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.cluster.logic.CapacityWriteNodesSelectionLogic
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbNode}
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest._

import scala.concurrent.duration._

class MetadataCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "MetadataCoordinatorSpec",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("60s"))
          .withValue("nsdb.write.scheduler.interval", ConfigValueFactory.fromAnyRef("20ms"))
      ))
    with ImplicitSender
    with NSDbSpecLike
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  import io.radicalbit.nsdb.cluster.coordinator.mockedActors.LocalMetadataCache._

  val writeNodesSelection = new CapacityWriteNodesSelectionLogic(
    CapacityWriteNodesSelectionLogic.fromConfigValue(system.settings.config.getString("nsdb.cluster.metrics-selector")))

  lazy val replicationFactor: Int = system.settings.config.getInt("nsdb.cluster.replication-factor")

  val probe                = TestProbe()
  val commitLogCoordinator = system.actorOf(Props[FakeCommitLogCoordinator])
  val schemaCache          = system.actorOf(Props[FakeSchemaCache])
  val schemaCoordinator =
    system.actorOf(SchemaCoordinator.props(schemaCache), "schemacoordinator")
  val metricsDataActorProbe = TestProbe()
  val metadataCache         = system.actorOf(Props[LocalMetadataCache])
  val clusterListener       = system.actorOf(Props[MockedClusterListener])

  val metadataCoordinator =
    system.actorOf(
      MetadataCoordinator.props(clusterListener, metadataCache, schemaCache, probe.ref, writeNodesSelection))

  val db        = "testDb"
  val namespace = "testNamespace"
  val metric    = "testMetric"

  val node1 = NSDbNode("localhost_2552", "node_01", "volatile1")
  val node2 = NSDbNode("localhost_2553", "node_02", "volatile2")

  val loc11 = Location(metric, node1, 0L, 30000L)
  val loc21 = Location(metric, node2, 0L, 30000L)
  val loc12 = Location(metric, node1, 30000L, 60000L)
  val loc22 = Location(metric, node2, 30000L, 60000L)

  implicit val timeout = Timeout(5 seconds)

  override def beforeAll = {
    NSDbClusterSnapshot(system).addNode(node1)
    NSDbClusterSnapshot(system).addNode(node2)

    metadataCoordinator ! SubscribeCommitLogCoordinator(commitLogCoordinator, node1.uniqueNodeId)
    awaitAssert {
      expectMsgType[CommitLogCoordinatorSubscribed]
    }
    metadataCoordinator ! SubscribeMetricsDataActor(metricsDataActorProbe.ref, node1.uniqueNodeId)
    awaitAssert {
      expectMsgType[MetricsDataActorSubscribed]
    }

    val nameRecord = Bit(0, 1, Map("name" -> "name"), Map("city" -> "milano"))
    schemaCoordinator ! UpdateSchemaFromRecord(db, namespace, metric, nameRecord)
    awaitAssert {
      expectMsgType[SchemaUpdated]
    }
  }

  override def beforeEach: Unit = {
    metadataCache ! DeleteAll
    awaitAssert {
      expectMsgType[DeleteDone.type]
    }
  }

  "MetadataCoordinator" should {
    "add multiple locations for a metric" in {

      val locations = Seq(Location(metric, node1, 0L, 30000L),
                          Location(metric, node1, 0L, 60000L),
                          Location(metric, node1, 0L, 90000L))

      probe.send(metadataCoordinator, AddLocations(db, namespace, locations))
      val locationAdded = awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      locationAdded.db shouldBe db
      locationAdded.namespace shouldBe namespace
      locationAdded.locations.sortBy(_.to) shouldBe locations

      probe.send(metadataCoordinator, GetLiveLocations(db, namespace, metric))
      awaitAssert {
        probe.expectMsgType[LiveLocationsGot].locations.sortBy(_.to) shouldBe locations
      }
    }

    "retrieve a Location for a metric" in {
      probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(Location(metric, node1, 0L, 30000L))))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      probe.send(metadataCoordinator, AddLocations(db, namespace, Seq(Location(metric, node2, 0L, 30000L))))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      probe.send(metadataCoordinator, GetLiveLocations(db, namespace, metric))
      val retrievedLocations = awaitAssert {
        probe.expectMsgType[LiveLocationsGot]
      }

      retrievedLocations.locations.size shouldBe 2
      val loc = retrievedLocations.locations.head
      loc.metric shouldBe metric
      loc.from shouldBe 0L
      loc.to shouldBe 30000L
      loc.node.uniqueNodeId shouldBe node1.uniqueNodeId

      val loc2 = retrievedLocations.locations.last
      loc2.metric shouldBe metric
      loc2.from shouldBe 0L
      loc2.to shouldBe 30000L
      loc2.node.uniqueNodeId shouldBe node2.uniqueNodeId
    }

    "retrieve Locations for a metric" in {

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

      probe.send(metadataCoordinator, GetLiveLocations(db, namespace, metric))
      val retrievedLocations = awaitAssert {
        probe.expectMsgType[LiveLocationsGot]
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
        probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 1, replicationFactor))
        val locationGot = probe.expectMsgType[WriteLocationsGot]
        locationGot.db shouldBe db
        locationGot.namespace shouldBe namespace
        locationGot.metric shouldBe metric

        locationGot.locations.size shouldBe 1
        locationGot.locations.foreach(_.from shouldBe 0L)
        locationGot.locations.foreach(_.to shouldBe 60000L)
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 60001, replicationFactor))
        val locationGot_2 = probe.expectMsgType[WriteLocationsGot]
        locationGot_2.db shouldBe db
        locationGot_2.namespace shouldBe namespace
        locationGot_2.metric shouldBe metric

        locationGot_2.locations.size shouldBe 1
        locationGot_2.locations.foreach(_.from shouldBe 60000L)
        locationGot_2.locations.foreach(_.to shouldBe 120000L)
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 60002, replicationFactor))
        val locationGot_3 = probe.expectMsgType[WriteLocationsGot]
        locationGot_3.db shouldBe db
        locationGot_3.namespace shouldBe namespace
        locationGot_3.metric shouldBe metric

        locationGot_3.locations.size shouldBe 1
        locationGot_3.locations.foreach(_.from shouldBe 60000L)
        locationGot_3.locations.foreach(_.to shouldBe 120000L)
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

      probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 1, replicationFactor))
      val locationGot = awaitAssert {
        probe.expectMsgType[WriteLocationsGot]
      }

      locationGot.db shouldBe db
      locationGot.namespace shouldBe namespace
      locationGot.metric shouldBe metric
      locationGot.locations.size shouldBe 1

      locationGot.locations.foreach(_.from shouldBe 0L)
      locationGot.locations.foreach(_.to shouldBe 100L)

      probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 101, replicationFactor))
      val locationGot_2 = awaitAssert {
        probe.expectMsgType[WriteLocationsGot]
      }

      locationGot_2.db shouldBe db
      locationGot_2.namespace shouldBe namespace
      locationGot_2.metric shouldBe metric

      locationGot_2.locations.size shouldBe 1
      locationGot_2.locations.foreach(_.from shouldBe 100L)
      locationGot_2.locations.foreach(_.to shouldBe 200L)

      probe.send(metadataCoordinator, GetWriteLocations(db, namespace, metric, 202, replicationFactor))
      val locationGot_3 = awaitAssert {
        probe.expectMsgType[WriteLocationsGot]
      }

      locationGot_3.db shouldBe db
      locationGot_3.namespace shouldBe namespace
      locationGot_3.metric shouldBe metric

      locationGot_3.locations.size shouldBe 1
      locationGot_3.locations.foreach(_.from shouldBe 200L)
      locationGot_3.locations.foreach(_.to shouldBe 300L)
    }

    "retrieve metric info" in {

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
        Location(metric, node1, 0L, 30000L),
        Location(metric, node1, 30000L, 60000L),
        Location(metric, node1, now - 5000, now - 2000),
        Location(metric, node1, now - 1000, now)
      )

      probe.send(metadataCoordinator, AddLocations(db, namespace, locations))
      awaitAssert {
        probe.expectMsgType[LocationsAdded]
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetLiveLocations(db, namespace, metric))
        val locationsGot = probe.expectMsgType[LiveLocationsGot]
        locationsGot.locations.sortBy(_.from) shouldBe locations
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetLiveLocations(db, namespace, metric))
        val locationsGot = probe.expectMsgType[LiveLocationsGot]
        locationsGot.locations.sortBy(_.from) shouldBe locations.takeRight(2)
      }

      awaitAssert {
        val deleteCommand = metricsDataActorProbe.expectMsgType[ExecuteDeleteStatementInternalInLocations]
        deleteCommand.locations shouldBe Seq(locations.takeRight(2).head)
      }

      awaitAssert {
        probe.send(metadataCoordinator, GetLiveLocations(db, namespace, metric))
        val locationsGot = probe.expectMsgType[LiveLocationsGot]
        locationsGot.locations.sortBy(_.from) shouldBe locations.takeRight(1)

        val deleteCommand = metricsDataActorProbe.expectMsgType[ExecuteDeleteStatementInternalInLocations]
        deleteCommand.locations shouldBe locations.takeRight(1)
      }
    }

    "exclude blacklisted nodes from location" in {

      metadataCoordinator ! AddLocations(db, namespace, Seq(loc11, loc12, loc21, loc22))
      awaitAssert {
        expectMsgType[LocationsAdded]
      }

      awaitAssert {
        metadataCoordinator ! GetLiveLocations(db, namespace, metric)
        val locations = expectMsgType[LiveLocationsGot].locations
        locations.size shouldBe 4
      }

      metadataCache ! AddNodeToBlackList(node1)
      awaitAssert {
        expectMsgType[NodeToBlackListAdded].node shouldBe node1
      }

      awaitAssert {
        metadataCoordinator ! GetLiveLocations(db, namespace, metric)
        val locations = expectMsgType[LiveLocationsGot].locations
        locations.size shouldBe 2
        locations should contain(loc21)
        locations should contain(loc22)
      }
    }
  }

}
