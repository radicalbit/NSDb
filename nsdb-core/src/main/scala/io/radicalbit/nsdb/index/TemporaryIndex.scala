package io.radicalbit.nsdb.index

import org.apache.lucene.store.RAMDirectory

class TemporaryIndex extends AbstractTimeSeriesIndex {
  override lazy val directory = new RAMDirectory()
}
