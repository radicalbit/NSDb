package io.radicalbit.nsdb.index.lucene

import java.util

import org.apache.lucene.index.{DocValues, LeafReaderContext, NumericDocValues, SortedDocValues}
import org.apache.lucene.search.grouping.AllGroupsCollector
import org.apache.lucene.util.{BytesRef, SentinelIntSet}

import scala.collection.mutable

object AllGroupsAggregationCollector {
  private val DEFAULT_INITIAL_SIZE = 128
}

abstract class AllGroupsAggregationCollector extends AllGroupsCollector[String] {

  val groupField: String
  val aggField: String
  val initialSize: Int = AllGroupsAggregationCollector.DEFAULT_INITIAL_SIZE

  protected val groups: mutable.Map[String, Long] = mutable.Map.empty
  protected val ordSet: SentinelIntSet            = new SentinelIntSet(initialSize, -2)
  protected var index: SortedDocValues            = _
  protected var aggIndex: NumericDocValues        = _

  override def getGroupCount: Int = groups.keys.size

  override def getGroups: util.Collection[String] = ???

  def getGroupMap: Map[String, Long] = groups.toMap

  override protected def doSetNextReader(context: LeafReaderContext): Unit = {
    index = DocValues.getSorted(context.reader, groupField)
    aggIndex = DocValues.getNumeric(context.reader, aggField)
    ordSet.clear()
    for (countedGroup <- groups) {
      if (countedGroup == null) ordSet.put(-1)
      else {
        val ord = index.lookupTerm(new BytesRef(countedGroup._1))
        if (ord >= 0) ordSet.put(ord)
      }
    }
  }
}
