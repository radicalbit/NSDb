package io.radicalbit.nsdb.commit_log

import akka.actor.Actor
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{Condition, DeleteSQLStatement, Expression}
import io.radicalbit.nsdb.index.Schema
import org.apache.lucene.search.Query

import scala.util.{Failure, Success, Try}

object CommitLogWriterActor {

  sealed trait CommitLoggerAction
  case class InsertAction(bit: Bit)                               extends CommitLoggerAction
  case class RejectAction(bit: Bit)                               extends CommitLoggerAction
  case class DeleteAction(deleteSQLStatement: DeleteSQLStatement) extends CommitLoggerAction
  case object DeleteNamespaceAction                               extends CommitLoggerAction
  case object DeleteMetricAction                                  extends CommitLoggerAction

  sealed trait CommitLogProtocol

  sealed trait CommitLogRequest  extends CommitLogProtocol
  sealed trait CommitLogResponse extends CommitLogProtocol

  case class WriteToCommitLog(db: String, namespace: String, metric: String, ts: Long, action: CommitLoggerAction)
      extends CommitLogRequest

  case class WriteToCommitLogSucceeded(db: String, namespace: String, ts: Long, metric: String)
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

  case class InsertEntry(db: String, namespace: String, metric: String, override val timestamp: Long, bit: Bit)
      extends CommitLogEntry
  case class DeleteEntry(db: String,
                         namespace: String,
                         metric: String,
                         override val timestamp: Long,
                         expression: Expression)
      extends CommitLogEntry
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
trait CommitLogWriterActor extends Actor {

  /**
    * Return an instance of a serializer extending [[CommitLogSerializer]] trait.
    * @return serializer component
    */
  protected def serializer: CommitLogSerializer

  final def receive: Receive = {
    case entry @ InsertEntry(db, namespace, metric, timestamp, bit) =>
      createEntry(entry) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, bit.timestamp, metric, ex.getMessage)
      }
    case entry @ DeleteEntry(db, namespace, metric, timestamp, _) =>
      createEntry(entry) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case entry @ RejectEntry(db, namespace, metric, timestamp, _) =>
      createEntry(entry) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case entry @ DeleteMetricEntry(db, namespace, metric, timestamp) =>
      createEntry(entry) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case entry @ DeleteNamespaceEntry(db, namespace, timestamp) =>
      createEntry(entry) match {
        case Success(_) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, "")
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
