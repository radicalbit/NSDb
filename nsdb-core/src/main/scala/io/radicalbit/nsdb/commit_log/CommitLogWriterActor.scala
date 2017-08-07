package io.radicalbit.nsdb.commit_log

import akka.actor.Actor

object CommitLogWriterActor {

  sealed trait CommitLogWriterActorProtocol

  case class WroteToCommitLogAck(ts: Long, metric: String) extends CommitLogWriterActorProtocol

}

trait CommitLogWriterActor extends Actor {

  protected def serializer: CommitLogSerializer

  final def receive = {
    case x: InsertNewEntry      => createEntry(x)
    case x: DeleteExistingEntry => deleteEntry(x)
  }

  protected def createEntry(commitLogEntry: InsertNewEntry): Unit

  protected def deleteEntry(commitLogEntry: DeleteExistingEntry): Unit
}
