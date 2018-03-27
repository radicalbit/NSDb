package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.JSerializable

/**
  * Trait that contains Long timestamp field.
  */
trait TimeSeriesRecord {
  val timestamp: Long
}

/**
  * Nsdb time series record.
  * A time series record is an object composed of:
  *
  * - a Long timestamp
  *
  * - a numeric value
  *
  * - a Map of generic dimensions (string or numeric values allowed).
  * @param timestamp record timestamp.
  * @param value record value.
  * @param dimensions record dimensions.
  */
case class Bit(timestamp: Long, value: JSerializable, dimensions: Map[String, JSerializable])
    extends TimeSeriesRecord {

  /**
    * @return all fields included dimensions, timestamp and value.
    */
  def fields: Map[String, JSerializable] =
    dimensions + ("timestamp" -> timestamp.asInstanceOf[JSerializable]) + ("value" -> value
      .asInstanceOf[JSerializable])
}
