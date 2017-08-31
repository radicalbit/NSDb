package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.JSerializable

trait TimeSeriesRecord {
  val timestamp: Long
}

case class Bit(timestamp: Long, value: JSerializable, dimensions: Map[String, JSerializable]) extends TimeSeriesRecord

case class BitOut(timestamp: Long, value: JSerializable, dimensions: Map[String, JSerializable])
    extends TimeSeriesRecord

object BitOut {
  def apply(record: Bit): BitOut =
    BitOut(timestamp = record.timestamp, value = record.value, dimensions = record.dimensions)
}
