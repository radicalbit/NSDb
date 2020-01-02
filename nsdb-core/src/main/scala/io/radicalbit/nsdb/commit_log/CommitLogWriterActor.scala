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

package io.radicalbit.nsdb.commit_log

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DeleteSQLStatement, Expression}
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.util.{Failure, Success, Try}

object CommitLogWriterActor {

  sealed trait CommitLogAction

  sealed trait BitEntryAction extends CommitLogAction {
    def bit: Bit
  }

  sealed trait LocationEntryAction extends CommitLogAction {
    def location: Location
  }

  case class ReceivedEntryAction(bit: Bit)                        extends BitEntryAction
  case class AccumulatedEntryAction(bit: Bit)                     extends BitEntryAction
  case class PersistedEntryAction(bit: Bit)                       extends BitEntryAction
  case class RejectedEntryAction(bit: Bit)                        extends BitEntryAction
  case class EvictShardAction(location: Location)                 extends LocationEntryAction
  case class DeleteAction(deleteSQLStatement: DeleteSQLStatement) extends CommitLogAction
  case object DeleteNamespaceAction                               extends CommitLogAction
  case object DeleteMetricAction                                  extends CommitLogAction

  sealed trait CommitLogProtocol

  sealed trait CommitLogRequest  extends CommitLogProtocol
  sealed trait CommitLogResponse extends CommitLogProtocol

  case class WriteToCommitLog(db: String,
                              namespace: String,
                              metric: String,
                              ts: Long,
                              action: CommitLogAction,
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
    *   Bit insertion events described in [[ReceivedEntry]], [[AccumulatedEntry]], [[PersistedEntry]], [[RejectedEntry]],
    *   Deletion operations as [[DeleteEntry]], MetricsDrop as [[DeleteMetricEntry]], NamespacesDrop as [[DeleteNamespaceEntry]].
    */
  sealed trait CommitLogEntry {
    def db: String
    def namespace: String
    def timestamp: Long
  }

  sealed trait CommitLogBitEntry extends CommitLogEntry {
    def id: Int
  }

  /**
    * To mark an entry that describe the end of a commit log record lifecycle. e.g. [[PersistedEntry]] and   [[RejectedEntry]]
    */
  sealed trait FinalizationEntry extends CommitLogBitEntry

  case object CommitLogBitEntry {
    case class ClassBitIdentifier(db: String, namespace: String, metric: String, bit: Bit)

    /**
      * Utility method used to generate an identifier for a bit
      * timestamp coming from [[CommitLogEntry]] is purposely not taken in account
      * used to intify persisted events related to the same logical [[Bit]]
      *
      * @param db database names
      * @param namespace namespace name
      * @param metric metric name
      * @param bit [[Bit]] entity
      * @return hash code representing the actual bit considering also the location (db, namespace, metric)
      */
    def bitIdentifier(db: String, namespace: String, metric: String, bit: Bit): Int = {
      val identifier = ClassBitIdentifier(db, namespace, metric, bit)
      identifier.hashCode()
    }
  }

  case class ReceivedEntry(db: String,
                           namespace: String,
                           metric: String,
                           override val timestamp: Long,
                           bit: Bit,
                           id: Int)
      extends CommitLogBitEntry
  case class AccumulatedEntry(db: String,
                              namespace: String,
                              metric: String,
                              override val timestamp: Long,
                              bit: Bit,
                              id: Int)
      extends CommitLogBitEntry
  case class PersistedEntry(db: String,
                            namespace: String,
                            metric: String,
                            override val timestamp: Long,
                            bit: Bit,
                            id: Int)
      extends FinalizationEntry
  case class RejectedEntry(db: String,
                           namespace: String,
                           metric: String,
                           override val timestamp: Long,
                           bit: Bit,
                           id: Int)
      extends FinalizationEntry
  case class DeleteEntry(db: String,
                         namespace: String,
                         metric: String,
                         override val timestamp: Long,
                         expression: Expression)
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

  def receive: Receive = {

    case WriteToCommitLog(db, namespace, metric, timestamp, bitEntryAction: ReceivedEntryAction, location) =>
      createEntry(
        ReceivedEntry(db,
                      namespace,
                      metric,
                      timestamp,
                      bitEntryAction.bit,
                      CommitLogBitEntry.bitIdentifier(db, namespace, metric, bitEntryAction.bit))) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }

    case WriteToCommitLog(db, namespace, metric, timestamp, bitEntryAction: AccumulatedEntryAction, location) =>
      createEntry(
        AccumulatedEntry(db,
                         namespace,
                         metric,
                         timestamp,
                         bitEntryAction.bit,
                         CommitLogBitEntry.bitIdentifier(db, namespace, metric, bitEntryAction.bit))) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case WriteToCommitLog(db, namespace, metric, timestamp, bitEntryAction: PersistedEntryAction, location) =>
      createEntry(
        PersistedEntry(db,
                       namespace,
                       metric,
                       timestamp,
                       bitEntryAction.bit,
                       CommitLogBitEntry.bitIdentifier(db, namespace, metric, bitEntryAction.bit))) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case WriteToCommitLog(db, namespace, metric, timestamp, bitEntryAction: RejectedEntryAction, location) =>
      createEntry(
        RejectedEntry(db,
                      namespace,
                      metric,
                      timestamp,
                      bitEntryAction.bit,
                      CommitLogBitEntry.bitIdentifier(db, namespace, metric, bitEntryAction.bit))) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }

    case _ @WriteToCommitLog(db, namespace, metric, timestamp, deleteEntryAction: DeleteAction, location) =>
      createEntry(DeleteEntry(db,
                              namespace,
                              metric,
                              timestamp,
                              deleteEntryAction.deleteSQLStatement.condition.expression)) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case WriteToCommitLog(db, namespace, metric, timestamp, DeleteMetricAction, location) =>
      createEntry(DeleteMetricEntry(db, namespace, metric, timestamp)) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric, location)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case WriteToCommitLog(db, namespace, _, timestamp, DeleteNamespaceAction, location) =>
      createEntry(DeleteNamespaceEntry(db, namespace, timestamp)) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, "", location)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, "", ex.getMessage)
      }
  }

  /**
    * Must be overridden defining specific commit-log persistence writes logic.
    *
    * @param commitLogEntry [[CommitLogEntry]] to be written
    * @return [[Try]] wrapping write result
    */
  protected def createEntry(commitLogEntry: CommitLogEntry): Try[Unit]

}
