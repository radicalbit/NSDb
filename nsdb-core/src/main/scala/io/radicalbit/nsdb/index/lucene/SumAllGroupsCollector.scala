package io.radicalbit.nsdb.index.lucene

import scala.reflect.ClassTag

class SumAllGroupsCollector[T: Numeric, S: Ordering: ClassTag](override val groupField: String,
                                                               override val aggField: String)
    extends AllGroupsAggregationCollector[T, S] {

  override def accumulateFunction(prev: T, actual: T): Option[T] = Some(implicitly[Numeric[T]].plus(prev, actual))

}
