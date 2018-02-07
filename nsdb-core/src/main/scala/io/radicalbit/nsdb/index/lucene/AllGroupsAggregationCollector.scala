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
  def accumulateFunction(prev: Long, actual: Long): Option[Long]

  val initialSize: Int = AllGroupsAggregationCollector.DEFAULT_INITIAL_SIZE

  protected val groups: mutable.Map[String, Long] = mutable.Map.empty
  protected val ordSet: SentinelIntSet            = new SentinelIntSet(initialSize, -2)
  protected var index: SortedDocValues            = _
  protected var aggIndex: NumericDocValues        = _

  override def getGroupCount: Int = groups.keys.size

  override def getGroups: util.Collection[String] = ???

  def getGroupMap: Map[String, Long] = groups.toMap

  def clear = {
    ordSet.clear()
    groups.clear()
    this
  }

  override def collect(doc: Int): Unit = {
    val key = index.getOrd(doc)

    val term: String =
      if (key == -1) null
      else BytesRef.deepCopyOf(index.lookupOrd(key)).utf8ToString()
    val agg = aggIndex.get(doc)

    if (!ordSet.exists(key)) {
      ordSet.put(key)
      groups += (term -> 1)
    } else {
      accumulateFunction(groups(term), agg).foreach(v => groups += (term -> v))
    }
  }

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

  def canEqual(other: Any): Boolean = other.getClass == this.getClass

  override def equals(other: Any): Boolean = other match {
    case that: AllGroupsAggregationCollector =>
      (that canEqual this) &&
        groupField == that.groupField &&
        aggField == that.aggField
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(groupField, aggField)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
