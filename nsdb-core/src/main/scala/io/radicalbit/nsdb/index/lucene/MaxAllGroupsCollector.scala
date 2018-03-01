package io.radicalbit.nsdb.index.lucene

class MaxAllGroupsCollector[T: Numeric](override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector[T] {

  override def accumulateFunction(prev: T, actual: T): Option[T] =
    if (implicitly[Numeric[T]].lt(prev, actual)) Some(actual) else Some(prev)

}
