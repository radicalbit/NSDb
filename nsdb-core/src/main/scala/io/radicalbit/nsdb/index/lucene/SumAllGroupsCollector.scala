package io.radicalbit.nsdb.index.lucene

class SumAllGroupsCollector(override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector {

  override def accumulateFunction(prev: Long, actual: Long): Option[Long] = Some(prev + actual)

}
