package io.radicalbit.nsdb.commit_log

import akka.actor.ActorRef
import io.radicalbit.nsdb.common.protocol.Bit

object CommitLogEntry {
  type DimensionName  = String
  type DimensionType  = String
  type DimensionValue = Array[Byte]
  type Dimension      = (DimensionName, DimensionType, DimensionValue)
}

sealed trait CommitLogEntry {
  def metric: String
  def bit: Bit
}

case class InsertNewEntry(override val metric: String, override val bit: Bit, replyTo: ActorRef) extends CommitLogEntry
case class NewEntryInserted(override val metric: String, override val bit: Bit)                  extends CommitLogEntry

case class CommitNewEntry(override val metric: String, override val bit: Bit, replyTo: ActorRef) extends CommitLogEntry
case class NewEntryCommitted(override val metric: String, override val bit: Bit)                 extends CommitLogEntry

case class RejectEntry(override val metric: String, override val bit: Bit, replyTo: ActorRef) extends CommitLogEntry
case class NewEntryRejected(override val metric: String, override val bit: Bit)               extends CommitLogEntry
