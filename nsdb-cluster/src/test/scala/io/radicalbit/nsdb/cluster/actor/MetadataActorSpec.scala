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

package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.index.MetricInfo
import io.radicalbit.nsdb.model.Location
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class FakeMetadataCoordinator extends Actor with ActorLogging {
  def receive: Receive = {
    case _ => log.info("received message")
  }
}

class MetadataActorSpec
    extends TestKit(ActorSystem("SchemaActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val probe               = TestProbe()
  val metadataCoordinator = system.actorOf(Props[FakeMetadataCoordinator])
  val metadataActor       = system.actorOf(MetadataActor.props("target/test_index/LocationActorTest", metadataCoordinator))

  lazy val db        = "db"
  lazy val namespace = "namespaceTest"
  lazy val metric    = "people"
  lazy val metric2   = "animal"

  lazy val locationsMetric = Seq(
    Location(metric, "node1", 0, 1),
    Location(metric, "node1", 2, 3),
    Location(metric, "node1", 4, 5),
    Location(metric, "node1", 6, 8)
  )

  lazy val locationsMetric2 = Seq(
    Location(metric2, "node1", 0, 1),
    Location(metric2, "node1", 2, 3),
    Location(metric2, "node1", 4, 5),
    Location(metric2, "node1", 6, 8)
  )

  before {
    implicit val timeout = Timeout(5 seconds)
    Await.result(metadataActor ? DeleteNamespaceMetadata(db, namespace), 5 seconds)
    Await.result(metadataActor ? AddLocations(db, namespace, locationsMetric ++ locationsMetric2), 5 seconds)
  }

  "MetadataActor" should "delete locations for a namespace" in {

    probe.send(metadataActor, DeleteNamespaceMetadata(db, namespace))

    val deleted = probe.expectMsgType[NamespaceMetadataDeleted]
    deleted.namespace shouldBe namespace

    probe.send(metadataActor, GetLocations(db, namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe Seq.empty
  }

  "MetadataActor" should "get locations for metric" in {

    probe.send(metadataActor, GetLocations(db, namespace, "nonexisting"))

    val nonexistingGot = probe.expectMsgType[LocationsGot]
    nonexistingGot.metric shouldBe "nonexisting"
    nonexistingGot.locations shouldBe Seq.empty

    probe.send(metadataActor, GetLocations(db, namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe locationsMetric
  }

  "MetadataActor" should "delete locations for metric" in {

    probe.send(metadataActor, GetLocations(db, namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe locationsMetric

    probe.send(metadataActor, DeleteMetricMetadata(db, namespace, metric))
    probe.expectMsgType[MetricMetadataDeleted]

    probe.send(metadataActor, GetLocations(db, namespace, metric))
    val afterDeleteLocations = probe.expectMsgType[LocationsGot]
    afterDeleteLocations.metric shouldBe metric
    afterDeleteLocations.locations shouldBe Seq()
  }

  "MetadataActor" should "add a new location" in {

    val newLocation = Location(metric, "node2", 10, 11)
    probe.send(metadataActor, AddLocation(db, namespace, newLocation))

    val added = probe.expectMsgType[LocationAdded]
    added.location shouldBe newLocation

    probe.send(metadataActor, GetLocations(db, namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe (locationsMetric :+ newLocation)
  }

  "MetadataActor" should "add a new metricInfo" in {
    val metricInfo = MetricInfo(metric, 10)

    probe.send(metadataActor, PutMetricInfo(db, namespace, metricInfo))
    probe.expectMsgType[MetricInfoPut].metricInfo shouldBe metricInfo

    probe.send(metadataActor, GetMetricInfo(db, namespace, metric))
    probe.expectMsgType[MetricInfoGot].metricInfo shouldBe Some(metricInfo)

    probe.send(metadataActor, PutMetricInfo(db, namespace, metricInfo))
    probe.expectMsgType[MetricInfoFailed].metricInfo shouldBe metricInfo
  }
}
