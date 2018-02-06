package io.radicalbit.nsdb.commit_log

import akka.actor.Actor
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WriteToCommitLogSucceeded
import io.radicalbit.nsdb.common.protocol.Bit

object CommitLogWriterActor {

  sealed trait CommitLogWriterResponse

  case class WriteToCommitLogSucceeded(ts: Long, metric: String, bit: Bit)              extends CommitLogWriterResponse
  case class WriteToCommitLogFailed(ts: Long, metric: String, bit: Bit, reason: String) extends CommitLogWriterResponse

}

trait CommitLogWriterActor extends Actor {

  protected def serializer: CommitLogSerializer

  final def receive = {
    case msg @ InsertNewEntry(metric, bit, replyTo) =>
      createEntry(msg)
      if (msg.replyTo != null) replyTo ! WriteToCommitLogSucceeded(bit.timestamp, metric, bit)
//    case x: DeleteExistingEntry => deleteEntry(x)
  }

  protected def createEntry(commitLogEntry: InsertNewEntry): Unit

//  protected def deleteEntry(commitLogEntry: DeleteExistingEntry): Unit
}
