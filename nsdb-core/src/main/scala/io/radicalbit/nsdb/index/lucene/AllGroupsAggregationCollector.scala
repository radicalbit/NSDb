package io.radicalbit.nsdb.index.lucene

import java.util

import io.radicalbit.nsdb.common.{JDouble, JLong}
import org.apache.lucene.document._
import org.apache.lucene.index._
import org.apache.lucene.search.SortField
import org.apache.lucene.search.grouping.AllGroupsCollector
import org.apache.lucene.util.BytesRef

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

object AllGroupsAggregationCollector {
  private val DEFAULT_INITIAL_SIZE = 128
}

abstract class AllGroupsAggregationCollector[T: Numeric, S: Ordering: ClassTag] extends AllGroupsCollector[S] {

  val groupField: String
  val aggField: String

  private val numeric = implicitly[Numeric[T]]

  def accumulateFunction(prev: T, actual: T): Option[T]

  val initialSize: Int = AllGroupsAggregationCollector.DEFAULT_INITIAL_SIZE

  protected val groups: mutable.Map[S, T]              = mutable.Map.empty
  protected var index: SortedDocValues                 = _
  protected var numericalIndex: NumericDocValues = _
  protected var aggIndex: NumericDocValues             = _

  override def getGroupCount: Int = groups.keys.size

  override def getGroups: util.Collection[S] = groups.keys.asJavaCollection

  def getGroupMap: Map[S, T] = groups.toMap

  private def Ord[F: Ordering](reverse: Boolean): Ordering[F] =
    if (reverse) implicitly[Ordering[F]].reverse else implicitly[Ordering[F]]

  def getOrderedMap(sortField: SortField): Map[S, T] = sortField match {
    case s if s.getType == SortField.Type.STRING =>
      getGroupMap.toSeq.sortBy(_._1)(Ord(s.getReverse)).toMap
    case s => getGroupMap.toSeq.sortBy(_._2)(Ord(s.getReverse)).toMap
  }

  def indexField[X](value: X): Field = value match {
    case v: Double => new DoublePoint(aggField, v)
    case v: Long   => new LongPoint(aggField, v)
    case v: Int    => new IntPoint(aggField, v)
    case v         => new StringField(aggField, v.toString, Field.Store.NO)
  }

  def fromBytes(bytesRef: BytesRef): S = {
    val className    = implicitly[ClassTag[S]].runtimeClass.getSimpleName
    val clazzString  = classOf[String].getSimpleName
    val clazzDouble  = classOf[JDouble].getSimpleName
    val clazzInteger = classOf[Integer].getSimpleName
    val clazzLong    = classOf[JLong].getSimpleName
    className match {
      case c if c == clazzDouble  => bytesRef.utf8ToString().toDouble.asInstanceOf[S]
      case c if c == clazzLong    => bytesRef.utf8ToString().toLong.asInstanceOf[S]
      case c if c == clazzInteger => bytesRef.utf8ToString().toInt.asInstanceOf[S]
      case c if c == clazzString  => bytesRef.utf8ToString().asInstanceOf[S]
    }
  }

  def clear: AllGroupsAggregationCollector[T, S] = {
    groups.clear()
    this
  }

  override def collect(doc: Int): Unit = {

    var stringGroup = false
    val className   = implicitly[ClassTag[S]].runtimeClass.getSimpleName
    val clazzString = classOf[String].getSimpleName
    val clazzDouble = classOf[JDouble].getSimpleName

    val term: S = className match {
      case c if c == clazzString =>
        val key = index.getOrd(doc)
        stringGroup = true
        fromBytes(BytesRef.deepCopyOf(index.lookupOrd(key)))
      case c if c == clazzDouble =>
        java.lang.Double.longBitsToDouble(numericalIndex.get(doc)).asInstanceOf[S]
      case _ => numericalIndex.get(doc).asInstanceOf[S]
    }

    val agg = numeric.one match {
      case _: Double => java.lang.Double.longBitsToDouble(aggIndex.get(doc)).asInstanceOf[T]
      case _         => aggIndex.get(doc).asInstanceOf[T]
    }

    if (!groups.contains(term)) {
      groups += (term -> agg)
    } else {
      accumulateFunction(groups(term), agg).foreach(v => groups += (term -> v))
    }
  }

  override protected def doSetNextReader(context: LeafReaderContext): Unit = {
    val className   = implicitly[ClassTag[S]].runtimeClass.getSimpleName
    val clazzString = classOf[String].getSimpleName

    className match {
      case c if c == clazzString =>
        index = DocValues.getSorted(context.reader, groupField)
      case _ =>
        numericalIndex = DocValues.getNumeric(context.reader, groupField)
    }

    aggIndex = DocValues.getNumeric(context.reader, aggField)
  }

  def canEqual(other: Any): Boolean = other.getClass == this.getClass

  override def equals(other: Any): Boolean = other match {
    case that: AllGroupsAggregationCollector[_, _] =>
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
