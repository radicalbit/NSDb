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

package io.radicalbit.nsdb.commit_log

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DeleteSQLStatement, Expression}
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.util.{Failure, Success, Try}

object CommitLogWriterActor {

  sealed trait CommitLoggerAction

  sealed trait BitEntryAction {
    def bit: Bit
  }

  case class ReceivedEntryAction(bit: Bit)    extends BitEntryAction
  case class AccumulatedEntryAction(bit: Bit) extends BitEntryAction
  case class PersistedEntryAction(bit: Bit)   extends BitEntryAction
  case class RejectedEntryAction(bit: Bit)    extends BitEntryAction

  case class InsertAction(bit: Bit)                               extends CommitLoggerAction
  case class RejectAction(bit: Bit)                               extends CommitLoggerAction
  case class DeleteAction(deleteSQLStatement: DeleteSQLStatement) extends CommitLoggerAction
  case object DeleteNamespaceAction                               extends CommitLoggerAction
  case object DeleteMetricAction                                  extends CommitLoggerAction

  sealed trait CommitLogProtocol

  sealed trait CommitLogRequest  extends CommitLogProtocol
  sealed trait CommitLogResponse extends CommitLogProtocol

  case class WriteToCommitLog(db: String,
                              namespace: String,
                              metric: String,
                              ts: Long,
                              action: CommitLoggerAction,
                              location: Location)
      extends CommitLogRequest

  case class WriteBitToCommitLog(db: String,
                                 namespace: String,
                                 metric: String,
                                 ts: Long,
                                 action: BitEntryAction,
                                 location: Location)
      extends CommitLogRequest

  case class WriteToCommitLogSucceeded(db: String, namespace: String, ts: Long, metric: String, location: Location)
      extends CommitLogResponse
  case class WriteToCommitLogFailed(db: String, namespace: String, ts: Long, metric: String, reason: String)
      extends CommitLogResponse

  object CommitLogEntry {
    type DimensionName  = String
    type DimensionType  = String
    type DimensionValue = Array[Byte]
    type ValueName      = String
    type ValueType      = String
    type RawValue       = Array[Byte]
    type Dimension      = (DimensionName, DimensionType, DimensionValue)
    type Value          = (ValueName, ValueType, RawValue)

  }

  /**
    * Describes entities serialized in commit-log files
    * The following entities are serialized in commit-logs:
    *   InsertedEntry described in [[InsertEntry]], RejectedEntry as [[RejectEntry]],
    *   EntryDeletion as [[DeleteEntry]], MetricsDrop as [[DeleteMetricEntry]], NamespacesDrop as [[DeleteNamespaceEntry]].
    */
  sealed trait CommitLogEntry {
    def db: String
    def namespace: String
    def timestamp: Long
  }

  sealed trait CommitLogOperation extends CommitLogEntry

  trait BitOperation extends CommitLogOperation {
    def metric: String
    def bit: Bit
  }

  case class ReceivedEntry(db: String, namespace: String, metric: String, override val timestamp: Long, bit: Bit)
      extends CommitLogEntry

  case class InsertEntry(db: String, namespace: String, metric: String, override val timestamp: Long, bit: Bit)
      extends CommitLogEntry
  case class DeleteEntry(db: String,
                         namespace: String,
                         metric: String,
                         override val timestamp: Long,
                         expression: Expression)
      extends CommitLogEntry
  //FIXME: remove useless message
  case class RejectEntry(db: String, namespace: String, metric: String, override val timestamp: Long, bit: Bit)
      extends CommitLogEntry
  case class DeleteMetricEntry(db: String, namespace: String, metric: String, override val timestamp: Long)
      extends CommitLogEntry
  case class DeleteNamespaceEntry(db: String, namespace: String, override val timestamp: Long) extends CommitLogEntry

}

/**
  * Trait describing CommitLogWriter behaviour
  * implemented by [[RollingCommitLogFileWriter]]
  *
  */
trait CommitLogWriterActor extends ActorPathLogging {

  /**
    * Return an instance of a serializer extending [[CommitLogSerializer]] trait.
    * @return serializer component
    */
  protected def serializer: CommitLogSerializer

  final def receive: Receive = {

    case entry @ WriteBitToCommitLog(db,
                                     namespace,
                                     metric,
                                     timestamp,
                                     bitEntryAction: ReceivedEntryAction,
                                     location) =>
      createEntry(ReceivedEntry(db, namespace, metric, timestamp, bitEntryAction.bit)) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }

//    case entry @ InsertEntry(db, namespace, metric, timestamp, bit, location) =>
//      createEntry(entry) match {
//        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
//        case Failure(ex) =>
//          sender() ! WriteToCommitLogFailed(db, namespace, bit.timestamp, metric, ex.getMessage)
//      }
//    case entry @ DeleteEntry(db, namespace, metric, timestamp, _, location) =>
//      createEntry(entry) match {
//        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
//        case Failure(ex) =>
//          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
//      }
//    case entry @ RejectEntry(db, namespace, metric, timestamp, _, location) =>
//      createEntry(entry) match {
//        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
//        case Failure(ex) =>
//          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
//      }
//    case entry @ DeleteMetricEntry(db, namespace, metric, timestamp, location) =>
//      createEntry(entry) match {
//        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
//        case Failure(ex) =>
//          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
//      }
//    case entry @ DeleteNamespaceEntry(db, namespace, timestamp, location) =>
//      createEntry(entry) match {
//        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, "", location)
//        case Failure(ex) =>
//          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, "", ex.getMessage)
//      }

  }

  /**
    * Must be overridden defining specific commit-log persistence writes logic.
    *
    * @param commitLogEntry [[CommitLogEntry]] to be written
    * @return [[Try]] wrapping write result
    */
  protected def createEntry(commitLogEntry: CommitLogEntry): Try[Unit]

}
