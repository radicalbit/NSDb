package io.radicalbit.nsdb.index

import org.apache.lucene.store.RAMDirectory

/**
  * Concrete implementation of [[AbstractTimeSeriesIndex]] which store data in memory.
  */
class TemporaryIndex extends AbstractTimeSeriesIndex {
  override lazy val directory = new RAMDirectory()
}
