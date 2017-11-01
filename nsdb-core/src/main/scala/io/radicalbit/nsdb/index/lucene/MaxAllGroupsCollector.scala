package io.radicalbit.nsdb.index.lucene

class MaxAllGroupsCollector(override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector {

  override def accumulateFunction(prev: Long, actual: Long): Option[Long] = if (prev < actual) Some(actual) else None

}
