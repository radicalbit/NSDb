package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.index.TimeSerieRecord

case class Record(timestamp: Long, dimensions: Map[String, Any], fields: Map[String, Any]) extends TimeSerieRecord
