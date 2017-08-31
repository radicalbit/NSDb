package io.radicalbit.commit_log

import io.radicalbit.nsdb.common.protocol.Bit

object CommitLogEntry {
  type DimensionName  = String
  type DimensionType  = String
  type DimensionValue = Array[Byte]
  type Dimension      = (DimensionName, DimensionType, DimensionValue)
}

sealed trait CommitLogEntry {
  def ts: Long
  def metric: String
}

case class InsertNewEntry(override val ts: Long, override val metric: String, record: Bit) extends CommitLogEntry

case class DeleteExistingEntry(override val ts: Long, override val metric: String) extends CommitLogEntry
