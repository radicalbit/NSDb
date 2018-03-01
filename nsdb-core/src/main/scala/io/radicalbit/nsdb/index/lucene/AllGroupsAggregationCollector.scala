package io.radicalbit.nsdb.index.lucene

import java.util

import org.apache.lucene.document._
import org.apache.lucene.index.{DocValues, LeafReaderContext, NumericDocValues, SortedDocValues}
import org.apache.lucene.search.SortField
import org.apache.lucene.search.grouping.AllGroupsCollector
import org.apache.lucene.util.{BytesRef, SentinelIntSet}

import scala.collection.JavaConverters._
import scala.collection.mutable

object AllGroupsAggregationCollector {
  private val DEFAULT_INITIAL_SIZE = 128
}

abstract class AllGroupsAggregationCollector[T: Numeric] extends AllGroupsCollector[String] {

  val groupField: String
  val aggField: String

  private val numeric = implicitly[Numeric[T]]

  def accumulateFunction(prev: T, actual: T): Option[T]

  val initialSize: Int = AllGroupsAggregationCollector.DEFAULT_INITIAL_SIZE

  protected val groups: mutable.Map[String, T] = mutable.Map.empty
  protected val ordSet: SentinelIntSet         = new SentinelIntSet(initialSize, -2)
  protected var index: SortedDocValues         = _
  protected var aggIndex: NumericDocValues     = _

  override def getGroupCount: Int = groups.keys.size

  override def getGroups: util.Collection[String] = groups.keys.asJavaCollection

  def getGroupMap: Map[String, T] = groups.toMap

  private def Ord[F: Ordering](reverse: Boolean): Ordering[F] =
    if (reverse) implicitly[Ordering[F]].reverse else implicitly[Ordering[F]]

  def getOrderedMap(sortField: SortField): Map[String, T] = sortField match {
    case s if s.getType == SortField.Type.STRING =>
      getGroupMap.toSeq.sortBy(_._1)(Ord(s.getReverse)).toMap
    case s => getGroupMap.toSeq.sortBy(_._2)(Ord(s.getReverse)).toMap
  }

  def indexField(value: T): Field = value match {
    case v: Double => new DoublePoint(aggField, v)
    case v: Long   => new LongPoint(aggField, v)
    case v: Int    => new IntPoint(aggField, v)
    case v         => new StringField(aggField, v.toString, Field.Store.NO)
  }

  def clear: AllGroupsAggregationCollector[T] = {
    ordSet.clear()
    groups.clear()
    this
  }

  override def collect(doc: Int): Unit = {
    val key = index.getOrd(doc)

    val term: String =
      if (key == -1) null
      else BytesRef.deepCopyOf(index.lookupOrd(key)).utf8ToString()

    val agg = numeric.one match {
      case _: Double => java.lang.Double.longBitsToDouble(aggIndex.get(doc)).asInstanceOf[T]
      case _         => aggIndex.get(doc).asInstanceOf[T]
    }

    if (!ordSet.exists(key)) {
      ordSet.put(key)
      groups += (term -> agg)
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
    case that: AllGroupsAggregationCollector[_] =>
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
