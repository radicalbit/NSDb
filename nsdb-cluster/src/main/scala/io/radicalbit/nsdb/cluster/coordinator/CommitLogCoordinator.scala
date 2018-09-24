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

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import io.radicalbit.nsdb.actors.MetricPerformerActor
import io.radicalbit.nsdb.actors.MetricPerformerActor.PersistedBits
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.commit_log.{CommitLogWriterActor, RollingCommitLogFileWriter}
import io.radicalbit.nsdb.common.protocol.Coordinates
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.collection.mutable
import scala.concurrent.Future
import akka.pattern.{ask, pipe}
import akka.util.Timeout

/**
  * Actor whose purpose is to handle writes on commit-log files delegating the action to writers implementing
  * [[CommitLogWriterActor]] trait .
  * In this implementation a writer is instantiated for each tuple (database, namespace).
  */
class CommitLogCoordinator extends ActorPathLogging {

  private val commitLoggerWriters: mutable.Map[Coordinates, ActorRef] = mutable.Map.empty

  private def getWriter(db: String, namespace: String, metric: String): ActorRef = {
    commitLoggerWriters.getOrElse(
      Coordinates(db, namespace, metric), {
        val commitLogWriter =
          context.actorOf(RollingCommitLogFileWriter.props(db, namespace, metric),
                          s"commit-log-writer-$db-$namespace-$metric")
        commitLoggerWriters += (Coordinates(db, namespace, metric) -> commitLogWriter)
        commitLogWriter
      }
    )
  }

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  def receive: Receive = {
    case msg @ WriteToCommitLog(db, namespace, metric, _, _, _) =>
      getWriter(db, namespace, metric).forward(msg)

    case persistedBits: PersistedBits =>
      import context.dispatcher

      // Handle successful events of Bit Persistence
      val successfullyPersistedBits: Seq[MetricPerformerActor.PersistedBit] = persistedBits.persistedBits.collect {
        case persistedBit if persistedBit.successfully => persistedBit
      }

      val successfulCommitLogResponses: Future[Seq[WriteToCommitLogSucceeded]] =
        Future.sequence {
          successfullyPersistedBits.map { persistedBit =>
            (getWriter(persistedBit.db, persistedBit.namespace, persistedBit.metric) ?
              WriteToCommitLog(persistedBit.db,
                               persistedBit.namespace,
                               persistedBit.metric,
                               persistedBit.timestamp,
                               PersistedEntryAction(persistedBit.bit),
                               persistedBit.location)).collect {
              case s: WriteToCommitLogSucceeded => s
            }
          }
//            writeCommitLog(
//              persistedBit.db,
//              persistedBit.namespace,
//              persistedBit.timestamp,
//              persistedBit.metric,
//              persistedBit.location.node,
//              PersistedEntryAction(persistedBit.bit),
//              persistedBit.location
//            ).collect {
//              case s: WriteToCommitLogSucceeded => s
//            }
        }
//        }

      val response = successfulCommitLogResponses.map { responses =>
        if (responses.size == successfullyPersistedBits.size)
          MetricPerformerActor.PersistedBitsAck
        else
          context.system.terminate()
      }
      response.pipeTo(sender())

    case _ =>
      log.error("UnexpectedMessage")
  }
}
