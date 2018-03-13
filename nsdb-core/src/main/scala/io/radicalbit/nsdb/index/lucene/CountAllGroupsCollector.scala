package io.radicalbit.nsdb.index.lucene

import scala.reflect.ClassTag

class CountAllGroupsCollector[S: Ordering: ClassTag](override val groupField: String, override val aggField: String)
    extends AllGroupsAggregationCollector[Long, S] {

  def accumulateFunction(prev: Long, actual: Long): Option[Long] = Some(prev + 1)

}
