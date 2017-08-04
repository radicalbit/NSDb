package io.radicalbit.model

import io.radicalbit.index.TimeSerieRecord

case class Record(timestamp: Long, dimensions: Map[String, Any], fields: Map[String, Any]) extends TimeSerieRecord
