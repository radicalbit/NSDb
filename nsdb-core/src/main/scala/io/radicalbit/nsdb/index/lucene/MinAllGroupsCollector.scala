package io.radicalbit.nsdb.index.lucene

import org.apache.lucene.document.Field

class MinAllGroupsCollector[T: Numeric](override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector[T] {

  override def accumulateFunction(prev: T, actual: T): Option[T] =
    if (implicitly[Numeric[T]].gteq(prev, actual)) Some(actual) else Some(prev)

  override def indexField(value: T): Field = ???
}
