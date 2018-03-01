package io.radicalbit.nsdb.index.lucene

class CountAllGroupsCollector(override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector[Long] {

  def accumulateFunction(prev: Long, actual: Long): Option[Long] = Some(prev + 1)

}
