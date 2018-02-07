package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.JSerializable

trait TimeSeriesRecord {
  val timestamp: Long
}

case class Bit(timestamp: Long, value: JSerializable, dimensions: Map[String, JSerializable])
    extends TimeSeriesRecord {

  def fields: Map[String, JSerializable] =
    dimensions + ("timestamp" -> timestamp.asInstanceOf[JSerializable]) + ("value" -> value
      .asInstanceOf[JSerializable])
}
