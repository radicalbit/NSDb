package io.radicalbit.nsdb.commit_log

import akka.actor.Actor
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit

import scala.util.{Failure, Success, Try}

object CommitLogWriterActor {

  sealed trait JournalServiceProtocol

  case class Insert(db: String, namespace: String, metric: String, bit: Bit) extends JournalServiceProtocol
  case class Commit(db: String, namespace: String, metric: String, bit: Bit) extends JournalServiceProtocol
  case class Reject(db: String, namespace: String, metric: String, bit: Bit) extends JournalServiceProtocol

  sealed trait JournalServiceResponse extends JournalServiceProtocol

  case class WriteToCommitLogSucceeded(db: String, namespace: String, ts: Long, metric: String, bit: Bit)
      extends JournalServiceResponse
  case class WriteToCommitLogFailed(db: String, namespace: String, ts: Long, metric: String, bit: Bit, reason: String)
      extends JournalServiceResponse

  sealed trait CommitLogWriterProtocol

  case class InsertNewEntry(entry: CommitLogEntry)   extends CommitLogWriterProtocol
  case class NewEntryInserted(entry: CommitLogEntry) extends CommitLogWriterProtocol

//  case class CommitNewEntry(metric: String, bit: Bit, replyTo: ActorRef) extends CommitLogWriterProtocol
//  case class NewEntryCommitted(metric: String, bit: Bit) extends CommitLogWriterProtocol

//  case class RejectNewEntry(metric: String, bit: Bit, replyTo: ActorRef) extends CommitLogWriterProtocol
//  case class NewEntryRejected(metric: String, bit: Bit) extends CommitLogWriterProtocol

}

trait CommitLogWriterActor extends Actor {

  protected def serializer: CommitLogSerializer

  final def receive: Receive = {
    case Insert(db, namespace, metric, bit) =>
      val commitLogEntry = InsertEntry(metric, bit)
      createEntry(commitLogEntry) match {
        case Success(r) => sender() ! WriteToCommitLogSucceeded(db, namespace, bit.timestamp, metric, bit)
        case Failure(ex) =>
          sender() ! WriteToCommitLogFailed(db, namespace, bit.timestamp, metric, bit, ex.getMessage)
      }

  }

  protected def createEntry(commitLogEntry: CommitLogEntry): Try[Unit]
}
