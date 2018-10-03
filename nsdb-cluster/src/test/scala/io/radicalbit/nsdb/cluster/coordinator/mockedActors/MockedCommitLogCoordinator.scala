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

package io.radicalbit.nsdb.cluster.coordinator.mockedActors

import java.time.Duration

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActorReads.AddRecordToLocation
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{GetLocations, GetWriteLocations}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsGot
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{
  WriteToCommitLog,
  WriteToCommitLogFailed,
  WriteToCommitLogSucceeded
}
import io.radicalbit.nsdb.common.protocol.Coordinates
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.DeleteRecordFromShard
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{RecordAdded, RecordRejected}

import scala.collection.mutable

class MockedCommitLogCoordinator(probe: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {
    case msg @ WriteToCommitLog(db, namespace, metric, timestamp, _, location)
        if location.node == "node1" && metric != "metric2" =>
      probe ! msg
      sender ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
    case msg @ WriteToCommitLog(db, namespace, metric, timestamp, _, location)
        if location.node == "node2" && metric != "metric2" =>
      probe ! msg
      sender ! WriteToCommitLogFailed(db, namespace, timestamp, metric, "mock failure reason")
    case msg @ WriteToCommitLog(db, namespace, metric, timestamp, _, location) =>
      probe ! msg
      sender ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
    case _ =>
      log.error("Not handled")
  }
}

case object MockedCommitLogCoordinator {
  def props(probe: ActorRef): Props =
    Props(new MockedCommitLogCoordinator(probe))
}

class MockedMetadataCoordinator extends Actor with ActorLogging {

  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  val locations: mutable.Map[(String, String), Seq[Location]] = mutable.Map.empty

  override def receive: Receive = {
    case GetLocations(db, namespace, metric) =>
      sender() ! LocationsGot(db, namespace, metric, locations.getOrElse((namespace, metric), Seq.empty))
    case GetWriteLocations(db, namespace, metric, timestamp) =>
      val locationNode1 =
        Location(Coordinates(db, namespace, metric), "node1", timestamp, timestamp + shardingInterval.toMillis)
      val locationNode2 =
        Location(Coordinates(db, namespace, metric), "node2", timestamp, timestamp + shardingInterval.toMillis)

      sender() ! LocationsGot(db, namespace, metric, Seq(locationNode1, locationNode2))
  }
}

class MockedMetricsDataActorWrites(probe: ActorRef) extends Actor with ActorLogging {

  override def receive: Receive = {
    case msg @ AddRecordToLocation(db, namespace, bit, location) if location.node == "node1" =>
      probe ! msg
      sender() ! RecordAdded(db, namespace, location.coordinates.metric, bit, location, System.currentTimeMillis())
    case msg @ AddRecordToLocation(db, namespace, bit, location) if location.node == "node2" =>
      probe ! msg
      sender() ! RecordRejected(db,
                                namespace,
                                location.coordinates.metric,
                                bit,
                                location,
                                List("errrrros"),
                                System.currentTimeMillis())
    case msg @ DeleteRecordFromShard(_, _, _, _) =>
      probe ! msg
  }
}

object MockedMetricsDataActorWrites {
  def props(probe: ActorRef): Props =
    Props(new MockedMetricsDataActorWrites(probe))
}
