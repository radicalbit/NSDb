package io.radicalbit.nsdb.index.lucene

import org.apache.lucene.util.BytesRef

class MinAllGroupsCollector(override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector {

  override def collect(doc: Int): Unit = {
    val key = index.getOrd(doc)

    val term: String =
      if (key == -1) null
      else BytesRef.deepCopyOf(index.lookupOrd(key)).utf8ToString()
    val agg = aggIndex.get(doc)

    if (!ordSet.exists(key)) {
      ordSet.put(key)
      groups += (term -> agg)
    } else {
      if (groups(term) >= agg) groups += (term -> agg)
    }
  }

}
