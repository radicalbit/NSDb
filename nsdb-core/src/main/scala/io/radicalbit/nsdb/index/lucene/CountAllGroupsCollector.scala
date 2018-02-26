package io.radicalbit.nsdb.index.lucene

import org.apache.lucene.document.Field

class CountAllGroupsCollector(override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector[Long] {

  def accumulateFunction(prev: Long, actual: Long): Option[Long] = Some(prev + 1)

  override def indexField(value: Long): Field = ???
}
