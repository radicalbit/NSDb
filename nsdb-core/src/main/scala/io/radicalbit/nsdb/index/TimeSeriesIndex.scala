package io.radicalbit.nsdb.index

import org.apache.lucene.store.BaseDirectory

/**
  * Concrete implementation of [[AbstractTimeSeriesIndex]] which stores file on disk.
  * @param directory index directory
  */
class TimeSeriesIndex(override val directory: BaseDirectory) extends AbstractTimeSeriesIndex {}
