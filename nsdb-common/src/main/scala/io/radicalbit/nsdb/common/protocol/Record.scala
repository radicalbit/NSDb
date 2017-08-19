package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.JSerializable

trait TimeSeriesRecord {
  val timestamp: Long
}

case class Record(timestamp: Long, dimensions: Map[String, JSerializable], fields: Map[String, JSerializable])
    extends TimeSeriesRecord

case class RecordOut(timestamp: Long, fields: Map[String, JSerializable]) extends TimeSeriesRecord
