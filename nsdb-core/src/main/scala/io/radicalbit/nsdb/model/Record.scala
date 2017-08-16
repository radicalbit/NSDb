package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.index.TimeSerieRecord

case class Record(timestamp: Long,
                  dimensions: Map[String, java.io.Serializable],
                  fields: Map[String, java.io.Serializable])
    extends TimeSerieRecord
