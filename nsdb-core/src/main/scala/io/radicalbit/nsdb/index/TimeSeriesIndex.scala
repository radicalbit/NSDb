package io.radicalbit.nsdb.index

import org.apache.lucene.store.BaseDirectory

class TimeSeriesIndex(override val directory: BaseDirectory) extends AbstractTimeSeriesIndex {}
