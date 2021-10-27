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

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.GetWriteLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.WriteLocationsGot
import io.radicalbit.nsdb.cluster.coordinator.MockedMetadataCoordinator._
import io.radicalbit.nsdb.cluster.coordinator.mockedActors._
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{RejectedEntryAction, WriteToCommitLog}
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbNode}
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{LiveLocationsGot, RecordRejected}
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

class MockedMetadataCoordinator extends Actor with ActorLogging {
  lazy val shardingInterval = context.system.settings.config.getDuration("nsdb.sharding.interval")

  val locations: mutable.Map[(String, String), Seq[Location]] = mutable.Map.empty

  override def receive: Receive = {
    case GetLiveLocations(db, namespace, metric) =>
      sender() ! LiveLocationsGot(db, namespace, metric, locations.getOrElse((namespace, metric), Seq.empty))
    case GetWriteLocations(db, namespace, metric, timestamp, _) =>
      val locationNode1 = Location(metric, node1, timestamp, timestamp + shardingInterval.toMillis)
      val locationNode2 = Location(metric, node2, timestamp, timestamp + shardingInterval.toMillis)

      sender() ! WriteLocationsGot(db, namespace, metric, Seq(locationNode1, locationNode2))
  }
}

object MockedMetadataCoordinator {
  val node1 = NSDbNode("localhost_2552", "node1", "volatile1")
  val node2 = NSDbNode("localhost_2553", "node2", "volatile2")
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
    with NSDbSpecLike
    with BeforeAndAfter
    with BeforeAndAfterAll {

  lazy val basePath = s"target/test_index/WriteCoordinatorErrorsSpec/${UUID.randomUUID}"

  val db        = "writeCoordinatorSpecDB"
  val namespace = "namespace"
  val metric1   = "metric1"
  val metric2   = "metric2"

  val callingProbe          = TestProbe("calling")
  val successCommitLogProbe = TestProbe("successCommitLog")
  val failureCommitLogProbe = TestProbe("failureCommitLog")

  val successAccumulationProbe = TestProbe("successAccumulation")
  val failureAccumulationProbe = TestProbe("failureAccumulation")

  implicit val timeout = Timeout(10 seconds)

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + 1.second

  lazy val successfulCommitLogCoordinator = system.actorOf(MockedCommitLogCoordinator.props(successCommitLogProbe.ref))
  lazy val failingCommitLogCoordinator    = system.actorOf(MockedCommitLogCoordinator.props(failureCommitLogProbe.ref))
  lazy val schemaCoordinator              = system.actorOf(SchemaCoordinator.props(system.actorOf(Props[FakeSchemaCache])))
  lazy val subscriber                     = system.actorOf(Props[TestSubscriber])
  lazy val publisherActor                 = system.actorOf(PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor])))
  lazy val mockedMetadataCoordinator      = system.actorOf(Props[MockedMetadataCoordinator])
  lazy val writeCoordinatorActor = system actorOf WriteCoordinator.props(mockedMetadataCoordinator,
                                                                         schemaCoordinator,
                                                                         system.actorOf(Props.empty))

  lazy val node1MetricsDataActor =
    system.actorOf(MockedMetricsDataActor.props(node1, node2, successAccumulationProbe.ref))
  lazy val node2MetricsDataActor =
    system.actorOf(MockedMetricsDataActor.props(node1, node2, failureAccumulationProbe.ref))

  val record1 = Bit(System.currentTimeMillis, 1, Map("dimension1" -> "dimension1"), Map("tag1" -> "tag1"))
  val record2 = Bit(System.currentTimeMillis, 2, Map("dimension2" -> "dimension2"), Map("tag2" -> "tag2"))

  override def beforeAll: Unit = {

    NSDbClusterSnapshot(system).addNode(node1)
    NSDbClusterSnapshot(system).addNode(node2)

    Await.result(
      writeCoordinatorActor ? SubscribeCommitLogCoordinator(successfulCommitLogCoordinator, node1.uniqueNodeId),
      10 seconds)
    Await.result(writeCoordinatorActor ? SubscribeCommitLogCoordinator(failingCommitLogCoordinator, node2.uniqueNodeId),
                 10 seconds)

    Await.result(writeCoordinatorActor ? SubscribeMetricsDataActor(node1MetricsDataActor, node1.uniqueNodeId),
                 10 seconds)
    Await.result(writeCoordinatorActor ? SubscribeMetricsDataActor(node2MetricsDataActor, node2.uniqueNodeId),
                 10 seconds)

    Await.result(writeCoordinatorActor ? SubscribePublisher(publisherActor, node1.uniqueNodeId), 10 seconds)
    Await.result(writeCoordinatorActor ? SubscribePublisher(publisherActor, node2.uniqueNodeId), 10 seconds)

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, metric1, record1), 10 seconds)
    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, metric2, record2), 10 seconds)
  }

  "WriteCoordinator" should {
    "handle failures during commit log writes" in {

      callingProbe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, metric1, record1))

      awaitAssert {
        successCommitLogProbe.expectMsgType[WriteToCommitLog]
      }

      awaitAssert {
        failureCommitLogProbe.expectMsgType[WriteToCommitLog]
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
    "handle failures during accumulation" in {

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
