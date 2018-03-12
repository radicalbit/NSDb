package io.radicalbit.nsdb.commit_log

import akka.actor.Actor
import io.radicalbit.nsdb.commit_log.CommitLogCoordinator.{WriteToCommitLogFailed, WriteToCommitLogSucceeded}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.Condition

import scala.util.{Failure, Success, Try}

object CommitLogWriterActor {

  object CommitLogEntry {
    type DimensionName  = String
    type DimensionType  = String
    type DimensionValue = Array[Byte]
    type ValueName = String
    type ValueType = String
    type RawValue = Array[Byte]
    type Dimension      = (DimensionName, DimensionType, DimensionValue)
    type Value = (ValueName, ValueType, RawValue)

  }

  sealed trait CommitLogEntry {
    def db: String,
    def namespace: String
    def timestamp: Long

  }

  case class InsertEntry(db: String, namespace: String, metric: String, override val timestamp: Long, bit: Bit)
      extends CommitLogEntry
  case class DeleteEntry(db: String,
                         namespace: String,
                         metric: String,
                         override val timestamp: Long,
                         condition: Condition)
      extends CommitLogEntry
  case class RejectEntry(db: String, namespace: String, metric: String, override val timestamp: Long, bit: Bit)
      extends CommitLogEntry
  case class DeleteMetricEntry(db: String, namespace: String, metric: String, override val timestamp: Long)
      extends CommitLogEntry
  case class DeleteNamespaceEntry(db: String, namespace: String, override val timestamp: Long) extends CommitLogEntry

}

trait CommitLogWriterActor extends Actor {

  protected def serializer: CommitLogSerializer

  final def receive: Receive = {
    case entry @ InsertEntry(db, namespace, metric, timestamp, bit) =>
      createEntry(entry) match {
        case Success(r) => sender() ! WriteToCommitLogSucceeded(db, namespace, bit.timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, bit.timestamp, metric, ex.getMessage)
      }
    case entry @ DeleteEntry(db, namespace, metric, timestamp, condition) =>
      createEntry(entry) match {
        case Success(r) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case entry @ RejectEntry(db, namespace, metric, timestamp, bit) =>
      createEntry(entry) match {
        case Success(r) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case entry @ DeleteMetricEntry(db, namespace, metric, timestamp) =>
      createEntry(entry) match {
        case Success(r) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, metric, ex.getMessage)
      }
    case entry @ DeleteNamespaceEntry(db, namespace, timestamp) =>
      createEntry(entry) match {
        case Success(r) => sender() ! WriteToCommitLogSucceeded(db, namespace, timestamp, "")
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, timestamp, "", ex.getMessage)
      }

  }

  protected def createEntry(commitLogEntry: CommitLogEntry): Try[Unit]

}
