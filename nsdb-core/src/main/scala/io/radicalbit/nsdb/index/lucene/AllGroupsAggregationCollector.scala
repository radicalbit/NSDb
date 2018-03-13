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

/**
  * Abstract class extended by specialized collectors, which override accumulateFunction method
  * in order to apply specific aggregation logic
  *
  * @tparam V aggregation value type
  * @tparam D group by dimension type
  */
abstract class AllGroupsAggregationCollector[V: Numeric, D: Ordering: ClassTag] extends AllGroupsCollector[D] {

  val groupField: String
  val aggField: String

  private val numeric = implicitly[Numeric[V]]

  def accumulateFunction(prev: V, actual: V): Option[V]

  val initialSize: Int = AllGroupsAggregationCollector.DEFAULT_INITIAL_SIZE

  protected val groups: mutable.Map[D, V]        = mutable.Map.empty
  protected var index: SortedDocValues           = _
  protected var numericalIndex: NumericDocValues = _
  protected var aggIndex: NumericDocValues       = _

  def valueRuntimeClass = implicitly[ClassTag[D]].runtimeClass

  /**
    * @return group count
    */
  override def getGroupCount: Int = groups.keys.size

  /**
    * @return groups identifiers
    */
  override def getGroups: util.Collection[D] = groups.keys.asJavaCollection

  /**
    * @return groups map
    */
  def getGroupMap: Map[D, V] = groups.toMap

  private def Ord[F: Ordering](reverse: Boolean): Ordering[F] =
    if (reverse) implicitly[Ordering[F]].reverse else implicitly[Ordering[F]]

  def getOrderedMap(sortField: SortField): Seq[(D, V)] = {
    val orderBy = sortField.getField
    sortField match {
      case s if groupField == orderBy =>
        getGroupMap.toSeq.sortBy(_._1)(Ord[D](s.getReverse))
      case s if aggField == orderBy => getGroupMap.toSeq.sortBy(_._2)(Ord[V](s.getReverse))
      case _                        => Seq.empty
    }
  }

  /**
    * Return a fields of type X for value and dimension's name
    *
    * @param value field value
    * @param field dimension name
    * @tparam X type of field value
    * @return the correct type field given value and dimension name
    */
  def indexField[X](value: X, field: String): Field = value match {
    case v: Double => new DoublePoint(field, v)
    case v: Long   => new LongPoint(field, v)
    case v: Int    => new IntPoint(field, v)
    case v         => new StringField(field, v.toString, Field.Store.NO)
  }

  /**
    * Return D from a byte representation
    *
    * @param bytesRef
    * @return group field of type D
    */
  def fromBytes(bytesRef: BytesRef): D = {
    val className    = implicitly[ClassTag[D]].runtimeClass.getSimpleName
    val clazzString  = classOf[String].getSimpleName
    val clazzDouble  = classOf[JDouble].getSimpleName
    val clazzInteger = classOf[Integer].getSimpleName
    val clazzLong    = classOf[JLong].getSimpleName
    className match {
      case c if c == clazzDouble  => bytesRef.utf8ToString().toDouble.asInstanceOf[D]
      case c if c == clazzLong    => bytesRef.utf8ToString().toLong.asInstanceOf[D]
      case c if c == clazzInteger => bytesRef.utf8ToString().toInt.asInstanceOf[D]
      case c if c == clazzString  => bytesRef.utf8ToString().asInstanceOf[D]
    }
  }

  /**
    * @return this instance clearing groups map
    */
  def clear: AllGroupsAggregationCollector[V, D] = {
    groups.clear()
    this
  }

  /**
    * Modifies groups map collecting group field and aggregation value from indexes
    *
    * @param doc
    */
  override def collect(doc: Int): Unit = {

    var stringGroup = false
    val className   = implicitly[ClassTag[D]].runtimeClass.getSimpleName
    val clazzString = classOf[String].getSimpleName
    val clazzDouble = classOf[JDouble].getSimpleName

    val term: D = className match {
      case c if c == clazzString =>
        val key = index.getOrd(doc)
        stringGroup = true
        fromBytes(BytesRef.deepCopyOf(index.lookupOrd(key)))
      case c if c == clazzDouble =>
        java.lang.Double.longBitsToDouble(numericalIndex.get(doc)).asInstanceOf[D]
      case _ => numericalIndex.get(doc).asInstanceOf[D]
    }

    val agg = numeric.one match {
      case _: Double => java.lang.Double.longBitsToDouble(aggIndex.get(doc)).asInstanceOf[V]
      case _         => aggIndex.get(doc).asInstanceOf[V]
    }

    if (!groups.contains(term)) {
      groups += (term -> agg)
    } else {
      accumulateFunction(groups(term), agg).foreach(v => groups += (term -> v))
    }
  }

  override protected def doSetNextReader(context: LeafReaderContext): Unit = {
    val className   = implicitly[ClassTag[D]].runtimeClass.getSimpleName
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
