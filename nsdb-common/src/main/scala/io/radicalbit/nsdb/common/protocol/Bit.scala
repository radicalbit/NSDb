package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.JSerializable

trait TimeSeriesRecord {
  val timestamp: Long
}

case class Bit(timestamp: Long, dimensions: Map[String, JSerializable], metric: JSerializable) extends TimeSeriesRecord

case class BitOut(timestamp: Long, fields: Map[String, JSerializable]) extends TimeSeriesRecord

object BitOut {
  def apply(record: Bit): BitOut =
    BitOut(timestamp = record.timestamp, fields = record.dimensions + ("value" -> record.metric))
}
