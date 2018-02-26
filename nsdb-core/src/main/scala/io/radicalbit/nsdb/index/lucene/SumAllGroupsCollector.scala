package io.radicalbit.nsdb.index.lucene

import org.apache.lucene.document.Field

class SumAllGroupsCollector[T: Numeric](override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector[T] {

  override def accumulateFunction(prev: T, actual: T): Option[T] = Some(implicitly[Numeric[T]].plus(prev, actual))

  override def indexField(value: T): Field = ???
}
