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

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.coordinator.FakeCache.{DeleteAll, DeleteDone}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{
  AddLocation,
  GetLocations,
  GetWriteLocation,
  WarmUpLocations
}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{LocationAdded, LocationGot, LocationsGot}
import io.radicalbit.nsdb.cluster.index.Location
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import akka.pattern._
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

class FakeCache extends Actor {

  val locations: mutable.Map[LocationKey, Location] = mutable.Map.empty[LocationKey, Location]

  def receive: Receive = {
    case PutLocationInCache(key, value) =>
      locations.put(key, value)
      sender ! LocationCached(key, Some(value))
    case GetLocationsFromCache(key) =>
      val locs: Seq[Location] = locations.values.filter(_.metric == key.metric).toSeq
      sender ! LocationsCached(key, locs)
    case DeleteAll =>
      locations.keys.foreach(k => locations.remove(k))
      sender() ! DeleteDone
  }
}

object FakeCache {
  def props: Props = Props(new FakeCache)

  case object DeleteAll
  case object DeleteDone
}

class MetadataCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "MetadataCoordinatorSpec",
        ConfigFactory
          .load()
          .withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
          .withValue("nsdb.metadata-coordinator.timeout", ConfigValueFactory.fromAnyRef("10s"))
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("60s"))
      ))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  import FakeCache._

  val probe               = TestProbe()
  val metadataCache       = system.actorOf(FakeCache.props)
  val metadataCoordinator = system.actorOf(MetadataCoordinator.props(metadataCache))

  val db        = "testDb"
  val namespace = "testNamespace"
  val metric    = "testMetric"

  override def beforeAll = {
    probe.send(metadataCoordinator, WarmUpLocations(List.empty))
    probe.expectNoMessage(1 second)
  }

  override def beforeEach: Unit = {
    implicit val timeout = Timeout(5 seconds)
    Await.result(metadataCache ? DeleteAll, 5 seconds)
  }

  "MetadataCoordinator" should {
    "start in warm-up and then change state" in {

      probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
      val cachedLocation: LocationsGot = awaitAssert {
        probe.expectMsgType[LocationsGot]
      }

      cachedLocation.locations.size shouldBe 0
    }

    "add a Location" in {
      probe.send(metadataCoordinator, AddLocation(db, namespace, Location(metric, "node_01", 0L, 60000L)))
      val locationAdded = awaitAssert {
        probe.expectMsgType[LocationAdded]
      }

      locationAdded.db shouldBe db
      locationAdded.namespace shouldBe namespace
      locationAdded.location shouldBe Location(metric, "node_01", 0L, 60000L)
    }

    "retrieve a Location for a metric" in {
      probe.send(metadataCoordinator, AddLocation(db, namespace, Location(metric, "node_01", 0L, 30000L)))
      awaitAssert {
        probe.expectMsgType[LocationAdded]
      }

      probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
      val retrievedLocations = awaitAssert {
        probe.expectMsgType[LocationsGot]
      }

      retrievedLocations.locations.size shouldBe 1
      val loc = retrievedLocations.locations.head
      loc.metric shouldBe metric
      loc.from shouldBe 0L
      loc.to shouldBe 30000L
      loc.node shouldBe "node_01"
    }

    "retrieve Locations for a metric" in {
      probe.send(metadataCoordinator, AddLocation(db, namespace, Location(metric, "node_01", 0L, 30000L)))
      awaitAssert {
        probe.expectMsgType[LocationAdded]
      }

      probe.send(metadataCoordinator, AddLocation(db, namespace, Location(metric, "node_01", 30000L, 60000L)))
      awaitAssert {
        probe.expectMsgType[LocationAdded]
      }

      probe.send(metadataCoordinator, GetLocations(db, namespace, metric))
      val retrievedLocations = awaitAssert {
        probe.expectMsgType[LocationsGot]
      }

      retrievedLocations.locations.size shouldBe 2
      val loc = retrievedLocations.locations
      loc.map(_.metric) shouldBe Seq(metric, metric)
      loc.map(_.from) shouldBe Seq(0L, 30000L)
      loc.map(_.to) shouldBe Seq(30000L, 60000L)
      loc.map(_.node) shouldBe Seq("node_01", "node_01")
    }

    "retrieve correct write Location given a timestamp" in {
      val timestamp_1 = 1L
      val timestamp_2 = 60001L
      val timestamp_3 = 60002L

      probe.send(metadataCoordinator, GetWriteLocation(db, namespace, metric, timestamp_1))
      val locationGot = awaitAssert {
        probe.expectMsgType[LocationGot]
      }

      locationGot.db shouldBe db
      locationGot.namespace shouldBe namespace
      locationGot.metric shouldBe metric
      locationGot.location.isDefined shouldBe true
      locationGot.location.get.from shouldBe 0L
      locationGot.location.get.to shouldBe 60000L

      probe.send(metadataCoordinator, GetWriteLocation(db, namespace, metric, timestamp_2))
      val locationGot_2 = awaitAssert {
        probe.expectMsgType[LocationGot]
      }

      locationGot_2.db shouldBe db
      locationGot_2.namespace shouldBe namespace
      locationGot_2.metric shouldBe metric
      locationGot_2.location.isDefined shouldBe true
      locationGot_2.location.get.from shouldBe 60000L
      locationGot_2.location.get.to shouldBe 120000L

      probe.send(metadataCoordinator, GetWriteLocation(db, namespace, metric, timestamp_3))
      val locationGot_3 = awaitAssert {
        probe.expectMsgType[LocationGot]
      }

      locationGot_3.db shouldBe db
      locationGot_3.namespace shouldBe namespace
      locationGot_3.metric shouldBe metric
      locationGot_3.location.isDefined shouldBe true
      locationGot_3.location.get.to shouldBe 120000L
      locationGot_3.location.get.from shouldBe 60000L

    }
  }

}
