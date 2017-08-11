package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.JSerializable
import io.radicalbit.nsdb.index.{IndexType, TimeSeriesRecord}

case class RawField(name: String, value: JSerializable)
case class TypedField(name: String, indexType: IndexType[_], value: JSerializable)
case class SchemaField(name: String, indexType: IndexType[_])

case class Record(timestamp: Long, dimensions: Map[String, JSerializable], fields: Map[String, JSerializable])
    extends TimeSeriesRecord

case class RecordOut(timestamp: Long, fields: Map[String, JSerializable]) extends TimeSeriesRecord
