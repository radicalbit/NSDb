package io.radicalbit.nsdb.commit_log

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

case class InsertEntry(override val metric: String, override val bit: Bit) extends CommitLogEntry
case class DeleteEntry(override val metric: String, override val bit: Bit) extends CommitLogEntry
case class CommitEntry(override val metric: String, override val bit: Bit) extends CommitLogEntry
case class RejectEntry(override val metric: String, override val bit: Bit) extends CommitLogEntry
