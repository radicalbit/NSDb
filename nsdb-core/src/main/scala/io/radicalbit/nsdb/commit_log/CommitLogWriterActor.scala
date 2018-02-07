package io.radicalbit.nsdb.commit_log

import akka.actor.{Actor, ActorRef}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{InsertNewEntry, NewEntryInserted}

object CommitLogWriterActor {

  sealed trait CommitLogWriterProtocol

  case class InsertNewEntry(entry: CommitLogEntry, replyTo: ActorRef)   extends CommitLogWriterProtocol
  case class NewEntryInserted(entry: CommitLogEntry, replyTo: ActorRef) extends CommitLogWriterProtocol

//  case class CommitNewEntry(metric: String, bit: Bit, replyTo: ActorRef) extends CommitLogWriterProtocol
//  case class NewEntryCommitted(metric: String, bit: Bit) extends CommitLogWriterProtocol

//  case class RejectNewEntry(metric: String, bit: Bit, replyTo: ActorRef) extends CommitLogWriterProtocol
//  case class NewEntryRejected(metric: String, bit: Bit) extends CommitLogWriterProtocol

}

trait CommitLogWriterActor extends Actor {

  protected def serializer: CommitLogSerializer

  final def receive: Receive = {
    case msg @ InsertNewEntry(entry, replyTo) =>
      createEntry(msg.entry)
      if (msg.replyTo != null) replyTo ! NewEntryInserted(entry, replyTo)
  }

  protected def createEntry(commitLogEntry: CommitLogEntry): Unit
}
