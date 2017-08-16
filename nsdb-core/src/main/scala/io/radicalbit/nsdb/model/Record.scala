package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.JSerializable
import io.radicalbit.nsdb.index.TimeSerieRecord

case class Record(timestamp: Long, dimensions: Map[String, JSerializable], fields: Map[String, JSerializable])
    extends TimeSerieRecord

case class RecordOut(timestamp: Long, fields: Map[String, JSerializable]) extends TimeSerieRecord
