package io.radicalbit.nsdb.commit_log

import akka.actor.Actor
import io.radicalbit.nsdb.commit_log.CommitLogCoordinator.{WriteToCommitLogFailed, WriteToCommitLogSucceeded}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.Condition

import scala.util.{Failure, Success, Try}

object CommitLogWriterActor {

  sealed trait CommitWriterProtocol

  case class InsertBit(db: String, namespace: String, metric: String, bit: Bit) extends CommitWriterProtocol
  case class DeleteBit(db: String, namespace: String, metric: String, timestamp: Long, condition: Condition) extends CommitWriterProtocol
  case class RejectBit(db: String, namespace: String, metric: String, bit: Bit) extends CommitWriterProtocol
  case class DeleteMetric(db: String, namespace: String, metric: String, timestamp: Long) extends CommitWriterProtocol
  case class DeleteNamespace(db: String, namespace: String, timestamp: Long) extends CommitWriterProtocol


}

trait CommitLogWriterActor extends Actor {

  protected def serializer: CommitLogSerializer

  final def receive: Receive = {
    case InsertBit(db, namespace, metric, bit) =>
      val commitLogEntry = InsertEntry(metric, bit.timestamp, bit)
      createEntry(commitLogEntry) match {
        case Success(r) => sender() ! WriteToCommitLogSucceeded(db, namespace, bit.timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, bit.timestamp, metric, bit, ex.getMessage)
      }
    case DeleteBit(db, namespace, metric, timestamp,  condition) =>
      val commitLogEntry = DeleteEntry(metric, timestamp, bit)
      createEntry(commitLogEntry) match {
        case Success(r) => sender() ! WriteToCommitLogSucceeded(db, namespace, bit.timestamp, metric)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, bit.timestamp, metric, bit, ex.getMessage)
      }

  }

  protected def createEntry(commitLogEntry: CommitLogEntry): Try[Unit]
}
