package io.radicalbit.nsdb.commit_log

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.Condition

object CommitLogEntry {
  type DimensionName  = String
  type DimensionType  = String
  type DimensionValue = Array[Byte]
  type Dimension      = (DimensionName, DimensionType, DimensionValue)

}

sealed trait CommitLogEntry{
  def timestamp: Long
}


  case class InsertEntry(metric: String, override val timestamp: Long,  bit: Bit) extends CommitLogEntry
  case class DeleteEntry(metric: String, override val timestamp: Long, condition: Condition) extends CommitLogEntry
  case class RejectEntry(metric: String, override val timestamp: Long,  bit: Bit) extends CommitLogEntry
  case class DeleteMetric(metric: String, override val timestamp: Long) extends CommitLogEntry
  case class DeleteNamespace(override val timestamp: Long) extends CommitLogEntry
}


