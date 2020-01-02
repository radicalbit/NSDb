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

package io.radicalbit.nsdb.cluster.coordinator.mockedActors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.AddRecordToLocation
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{
  WriteToCommitLog,
  WriteToCommitLogFailed,
  WriteToCommitLogSucceeded
}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.DeleteRecordFromShard
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{RecordAdded, RecordRejected}

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

class MockedMetricsDataActor(probe: ActorRef) extends Actor with ActorLogging {

  override def receive: Receive = {
    case msg @ AddRecordToLocation(db, namespace, bit, location) if location.node == "node1" =>
      probe ! msg
      sender() ! RecordAdded(db, namespace, location.metric, bit, location, System.currentTimeMillis())
    case msg @ AddRecordToLocation(db, namespace, bit, location) if location.node == "node2" =>
      probe ! msg
      sender() ! RecordRejected(db,
                                namespace,
                                location.metric,
                                bit,
                                location,
                                List("errrrros"),
                                System.currentTimeMillis())
    case msg @ DeleteRecordFromShard(_, _, _, _) =>
      probe ! msg
  }
}

object MockedMetricsDataActor {
  def props(probe: ActorRef): Props =
    Props(new MockedMetricsDataActor(probe))
}
