package io.radicalbit.nsdb.index.lucene

import scala.reflect.ClassTag

class MaxAllGroupsCollector[T: Numeric, S: Ordering: ClassTag](override val groupField: String,
                                                               override val aggField: String)
    extends AllGroupsAggregationCollector[T, S] {

  override def accumulateFunction(prev: T, actual: T): Option[T] =
    if (implicitly[Numeric[T]].lt(prev, actual)) Some(actual) else Some(prev)

}
