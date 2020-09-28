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

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{GetLocations, GetWriteLocations}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{LocationsGot, WriteLocationsGot}
import io.radicalbit.nsdb.cluster.coordinator.mockedActors._
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{RejectedEntryAction, WriteToCommitLog}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.RecordRejected
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

class MockedMetadataCoordinator extends Actor with ActorLogging {

  lazy val shardingInterval = context.system.settings.config.getDuration("nsdb.sharding.interval")

  val locations: mutable.Map[(String, String), Seq[Location]] = mutable.Map.empty

  override def receive: Receive = {
    case GetLocations(db, namespace, metric) =>
      sender() ! LocationsGot(db, namespace, metric, locations.getOrElse((namespace, metric), Seq.empty))
    case GetWriteLocations(db, namespace, metric, timestamp) =>
      val locationNode1 = Location(metric, "node1", timestamp, timestamp + shardingInterval.toMillis)
      val locationNode2 = Location(metric, "node2", timestamp, timestamp + shardingInterval.toMillis)

      sender() ! WriteLocationsGot(db, namespace, metric, Seq(locationNode1, locationNode2))
  }
}

class WriteCoordinatorErrorsSpec
    extends TestKit(
      ActorSystem(
        "WriteCoordinatorErrorsSpec",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
      ))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll {

  lazy val basePath = "target/test_index/WriteCoordinatorErrorsSpec"

  val db        = "writeCoordinatorSpecDB"
  val namespace = "namespace"
  val metric1   = "metric1"
  val metric2   = "metric2"

  final val node1 = "node1"
  final val node2 = "node2"

  val callingProbe          = TestProbe()
  val successCommitLogProbe = TestProbe()
  val failureCommitLogProbe = TestProbe()

  val successAccumulationProbe = TestProbe()
  val failureAccumulationProbe = TestProbe()

  implicit val timeout = Timeout(10 seconds)

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + 1.second

  lazy val successfulCommitLogCoordinator =
    TestActorRef[MockedCommitLogCoordinator](MockedCommitLogCoordinator.props(successCommitLogProbe.ref))
  lazy val failingCommitLogCoordinator: TestActorRef[MockedCommitLogCoordinator] =
    TestActorRef[MockedCommitLogCoordinator](MockedCommitLogCoordinator.props(failureCommitLogProbe.ref))
  lazy val schemaCoordinator =
    TestActorRef[SchemaCoordinator](SchemaCoordinator.props(system.actorOf(Props[FakeSchemaCache])))
  lazy val subscriber = TestActorRef[TestSubscriber](Props[TestSubscriber])
  lazy val publisherActor =
    TestActorRef[PublisherActor](PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor])))
  lazy val mockedMetadataCoordinator = system.actorOf(Props[MockedMetadataCoordinator])
  lazy val writeCoordinatorActor = system actorOf WriteCoordinator.props(mockedMetadataCoordinator,
                                                                         schemaCoordinator,
                                                                         system.actorOf(Props.empty))

  lazy val node1MetricsDataActor =
    TestActorRef[MetricsDataActor](MockedMetricsDataActor.props(successAccumulationProbe.ref))
  lazy val node2MetricsDataActor =
    TestActorRef[MetricsDataActor](MockedMetricsDataActor.props(failureAccumulationProbe.ref))

  val record1 = Bit(System.currentTimeMillis, 1, Map("dimension1" -> "dimension1"), Map("tag1" -> "tag1"))
  val record2 = Bit(System.currentTimeMillis, 2, Map("dimension2" -> "dimension2"), Map("tag2" -> "tag2"))

  override def beforeAll: Unit = {
    Await.result(writeCoordinatorActor ? SubscribeCommitLogCoordinator(successfulCommitLogCoordinator, node1),
                 10 seconds)
    Await.result(writeCoordinatorActor ? SubscribeCommitLogCoordinator(failingCommitLogCoordinator, node2), 10 seconds)

    Await.result(writeCoordinatorActor ? SubscribeMetricsDataActor(node1MetricsDataActor, node1), 10 seconds)
    Await.result(writeCoordinatorActor ? SubscribeMetricsDataActor(node2MetricsDataActor, node2), 10 seconds)

    Await.result(writeCoordinatorActor ? SubscribePublisher(publisherActor, node1), 10 seconds)
    Await.result(writeCoordinatorActor ? SubscribePublisher(publisherActor, node2), 10 seconds)

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, metric1, record1), 10 seconds)
    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, metric2, record2), 10 seconds)
  }

  "WriteCoordinator" should {
    "handle failures during commit log writes" in within(5.seconds) {

      callingProbe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, metric1, record1))

      awaitAssert {
        successCommitLogProbe.expectMsgType[WriteToCommitLog]
      }

      awaitAssert {
        failureCommitLogProbe.expectMsgType[WriteToCommitLog]
      }

      val compensationMessage = awaitAssert {
        successCommitLogProbe.expectMsgType[WriteToCommitLog]
      }

      compensationMessage.action.shouldBe(RejectedEntryAction(record1))

      awaitAssert {
        callingProbe.expectMsgType[RecordRejected]
      }
    }
    "handle failures during accumulation" in within(5.seconds) {

      callingProbe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, metric2, record2))

      awaitAssert {
        successCommitLogProbe.expectMsgType[WriteToCommitLog]
      }

      awaitAssert {
        failureCommitLogProbe.expectMsgType[WriteToCommitLog]
      }

      awaitAssert {
        successAccumulationProbe.expectMsgType[AddRecordToShard]
      }

      awaitAssert {
        failureAccumulationProbe.expectMsgType[AddRecordToShard]
      }

      awaitAssert {
        successAccumulationProbe.expectMsgType[DeleteRecordFromShard]
      }

      val rejectionRequestNode1 = awaitAssert {
        successCommitLogProbe.expectMsgType[WriteToCommitLog]
      }

      rejectionRequestNode1.action.shouldBe(RejectedEntryAction(record2))

      val rejectionRequestNode2 = awaitAssert {
        failureCommitLogProbe.expectMsgType[WriteToCommitLog]
      }

      rejectionRequestNode2.action.shouldBe(RejectedEntryAction(record2))

      awaitAssert {
        callingProbe.expectMsgType[RecordRejected]
      }

    }

  }

}
