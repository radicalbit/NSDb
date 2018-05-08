/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.commit_log

import java.nio.{Buffer, ByteBuffer}

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry.{Dimension, Value}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}

/**
  * Utility class to Serialize and Deserialize a CommitLogEntry.
  * Please note that this class is not intended to be thread safe.
  */
class StandardCommitLogSerializer extends CommitLogSerializer with TypeSupport {

  private val readByteBuffer = new ReadBuffer(5000)
  private val writeBuffer    = new WriteBuffer(5000)

  private final val rangeExpressionClazzName        = classOf[RangeExpression[_]].getCanonicalName
  private final val comparisonExpressionClassName   = classOf[ComparisonExpression[_]].getCanonicalName
  private final val equalityExpressionClassName     = classOf[EqualityExpression[_]].getCanonicalName
  private final val likeExpressionClassName         = classOf[LikeExpression].getCanonicalName
  private final val nullableExpressionClassName     = classOf[NullableExpression].getCanonicalName
  private final val unaryLogicalExpressionClassName = classOf[UnaryLogicalExpression].getCanonicalName
  private final val tupleLogicalExpressionClassName = classOf[TupledLogicalExpression].getCanonicalName

  /**
    * Dimensions serialization utility used to convert a Dimension's key-value representation
    * into a String one using [[Dimension]] type.
    *
    * @param dimensions [[Map]] containing bit's dimensions in key-value format
    * @return a list of [[Dimension]] each one representing [[io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry.DimensionName]],
    *         [[io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry.DimensionType]],
    *         [[io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry.DimensionValue]]
    */
  private def extractDimensions(dimensions: Map[String, JSerializable]): List[Dimension] =
    dimensions.map {
      case (k, v) =>
        val i = IndexType.fromClass(v.getClass).get
        (k, i.getClass.getCanonicalName, i.serialize(v))
    }.toList

  /**
    * Bit value serialization utility used to convert it to a serializable representation
    *
    * @param value bit value
    * @return serializable value representation composed of:
    *         [[io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry.ValueName]]
    *         [[io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry.ValueType]]
    *         [[io.radicalbit.nsdb.commit_log.CommitLogWriterActor.CommitLogEntry.RawValue]]
    */
  private def extractValue(value: JSerializable): Value = {
    val vType = IndexType.fromClass(value.getClass).get
    ("value", vType.getClass.getCanonicalName, vType.serialize(value))
  }

  /**
    * Dimensions deserialization utility used to convert a String representation using [[Dimension]] type
    * into a Dimension's key-value representation
    *
    * @param dimensions [[List]] containing bit's dimensions in [[Dimension]] type
    * @return [[Map]] containing bit's dimensions in key-value representation
    */
  private def createDimensions(dimensions: List[Dimension]): Map[String, JSerializable] =
    dimensions.map {
      case (n, t, v) =>
        val i = Class.forName(t).newInstance().asInstanceOf[IndexType[_]]
        n -> i.deserialize(v).asInstanceOf[JSerializable]
    }.toMap

  /**
    * Deserialize [[Expression]] simple values given the Class canonicalName
    *
    * @param clazz value class canonicalName
    * @return deserialized value instance casted into the correct class
    */
  private def argument(clazz: String): AnyRef = {
    val longClazz: String = classOf[java.lang.Long].getCanonicalName
    val intClazz          = classOf[java.lang.Integer].getCanonicalName
    val doubleClazz       = classOf[java.lang.Double].getCanonicalName
    val stringClazz       = classOf[java.lang.String].getCanonicalName

    clazz match {
      case `longClazz`   => Long.box(readByteBuffer.read.toLong)
      case `intClazz`    => Int.box(readByteBuffer.read.toInt)
      case `doubleClazz` => Double.box(readByteBuffer.read.toDouble)
      case `stringClazz` => readByteBuffer.read
    }
  }

  /**
    * Deserialize [[DeleteEntry.expression]] given the class canonicalName
    *
    * @param expressionClass class canonicalName
    * @return [[Expression]] representing delete query where condition
    */
  private def createExpression(expressionClass: String): Expression = {
    import scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val clazz = Class
      .forName(expressionClass)

    val expression = expressionClass match {
      case `rangeExpressionClazzName` =>
        val dim             = readByteBuffer.read
        val lowerBoundType  = readByteBuffer.read
        val lowerBoundValue = argument(lowerBoundType)
        val upperBoundType  = readByteBuffer.read
        val upperBoundValue = argument(upperBoundType)
        clazz
          .getConstructor(classOf[String], classOf[AnyRef], classOf[AnyRef])
          .newInstance(dim, lowerBoundValue, upperBoundValue)

      case `comparisonExpressionClassName` =>
        val dim       = readByteBuffer.read
        val opClazz   = readByteBuffer.read
        val module    = runtimeMirror.staticModule(opClazz)
        val operator  = runtimeMirror.reflectModule(module).instance.asInstanceOf[ComparisonOperator]
        val valueType = readByteBuffer.read
        val value     = argument(valueType)
        clazz
          .getConstructor(classOf[String], classOf[ComparisonOperator], classOf[AnyRef])
          .newInstance(dim, operator, value)

      case `equalityExpressionClassName` =>
        val dim       = readByteBuffer.read
        val valueType = readByteBuffer.read
        val value     = argument(valueType)
        clazz
          .getConstructor(classOf[String], classOf[AnyRef])
          .newInstance(dim, value)

      case `likeExpressionClassName` =>
        val dim       = readByteBuffer.read
        val valueType = readByteBuffer.read
        val value     = argument(valueType)
        clazz
          .getConstructor(classOf[String], classOf[String])
          .newInstance(dim, value)

      case `nullableExpressionClassName` =>
        val dim = readByteBuffer.read
        clazz
          .getConstructor(classOf[String])
          .newInstance(dim)

      case `unaryLogicalExpressionClassName` =>
        val expClass = readByteBuffer.read
        val exp      = createExpression(expClass)
        val opClazz  = readByteBuffer.read
        val module   = runtimeMirror.staticModule(opClazz)
        val operator = runtimeMirror.reflectModule(module).instance.asInstanceOf[SingleLogicalOperator]
        clazz
          .getConstructor(classOf[Expression], classOf[SingleLogicalOperator])
          .newInstance(exp, operator)

      case `tupleLogicalExpressionClassName` =>
        val expClass1 = readByteBuffer.read
        val exp1      = createExpression(expClass1)
        val opClazz   = readByteBuffer.read
        val module    = runtimeMirror.staticModule(opClazz)
        val operator  = runtimeMirror.reflectModule(module).instance.asInstanceOf[TupledLogicalOperator]
        val expClass2 = readByteBuffer.read
        val exp2      = createExpression(expClass2)
        clazz
          .getConstructor(classOf[Expression], classOf[TupledLogicalOperator], classOf[Expression])
          .newInstance(exp1, operator, exp2)

    }

    expression.asInstanceOf[Expression]
  }

  /**
    * Serializes an [[Expression]] into an Array[Byte] writing into [[WriteBuffer]]
    *
    * @param expression [[DeleteEntry.expression]] to be serialized
    * @return Array[Byte] representation also writing into writeBuffer
    */
  private def extractExpression(expression: Expression): Array[Byte] = {
    val clazzName = expression.getClass.getCanonicalName
    writeBuffer.write(clazzName)

    expression match {
      case ComparisonExpression(dimension, comparisonOperator, value) =>
        writeBuffer.write(dimension)
        writeBuffer.write(comparisonOperator.getClass.getCanonicalName)
        writeBuffer.write(value.getClass.getCanonicalName)
        writeBuffer.write(value.toString)
      case RangeExpression(dimension, value1, value2) =>
        writeBuffer.write(dimension)
        writeBuffer.write(value1.getClass.getCanonicalName)
        writeBuffer.write(value1.toString)
        writeBuffer.write(value2.getClass.getCanonicalName)
        writeBuffer.write(value2.toString)
      case EqualityExpression(dimension, value) =>
        writeBuffer.write(dimension)
        writeBuffer.write(value.getClass.getCanonicalName)
        writeBuffer.write(value.toString)
      case LikeExpression(dimension, value) =>
        writeBuffer.write(dimension)
        writeBuffer.write(value.getClass.getCanonicalName)
        writeBuffer.write(value.toString)
      case NullableExpression(dimension) =>
        writeBuffer.write(dimension)
      case UnaryLogicalExpression(expression1, unaryLogicalOperator) =>
        extractExpression(expression1)
        writeBuffer.write(unaryLogicalOperator.getClass.getCanonicalName)
      case TupledLogicalExpression(expression1, tupledLogicalOperator, expression2) =>
        extractExpression(expression1)
        writeBuffer.write(tupledLogicalOperator.getClass.getCanonicalName)
        extractExpression(expression2)
    }

    writeBuffer.array
  }

  /**
    * Deserialization methods for [[CommitLogEntry]]
    *
    * @param entry a Array[Byte] to be deserialized into a [[CommitLogEntry]]
    * @return the deserialize [[CommitLogEntry]]
    */
  override def deserialize(entry: Array[Byte]): CommitLogEntry = {
    readByteBuffer.clear(entry)
    //classname
    val className = readByteBuffer.read
    // timestamp
    val ts = readByteBuffer.read.toLong
    // database
    val db = readByteBuffer.read
    //  namespace
    val namespace = readByteBuffer.read

    className match {
      case c if c == classOf[InsertEntry].getCanonicalName =>
        // metric
        val metric = readByteBuffer.read
        // dimensions
        val numOfDim = readByteBuffer.getInt
        val dimensions = (for {
          _ <- 1 to numOfDim
          name  = readByteBuffer.read
          typ   = readByteBuffer.read
          value = new Array[Byte](readByteBuffer.getInt)
          _     = readByteBuffer.get(value)
        } yield (name, typ, value)).toList

        InsertEntry(db = db,
                    namespace = namespace,
                    metric = metric,
                    timestamp = ts,
                    Bit(timestamp = ts, value = 0, dimensions = createDimensions(dimensions)))
      case c if c == classOf[RejectEntry].getCanonicalName =>
        // metric
        val metric = readByteBuffer.read
        // dimensions
        val numOfDim = readByteBuffer.getInt
        val dimensions = (for {
          _ <- 1 to numOfDim
          name  = readByteBuffer.read
          typ   = readByteBuffer.read
          value = new Array[Byte](readByteBuffer.getInt)
          _     = readByteBuffer.get(value)
        } yield (name, typ, value)).toList

        RejectEntry(db = db,
                    namespace = namespace,
                    metric = metric,
                    timestamp = ts,
                    Bit(timestamp = ts, value = 0, dimensions = createDimensions(dimensions)))
      case c if c == classOf[DeleteEntry].getCanonicalName =>
        // metric
        val metric          = readByteBuffer.read
        val expressionClass = readByteBuffer.read
        DeleteEntry(db = db,
                    namespace = namespace,
                    metric = metric,
                    timestamp = ts,
                    expression = createExpression(expressionClass))

    }
  }

  /**
    * Serialization method for [[CommitLogEntry]]
    *
    * @param entry a [[CommitLogEntry]]
    * @return Array[Byte] representation
    */
  override def serialize(entry: CommitLogEntry): Array[Byte] =
    entry match {
      case e: InsertEntry =>
        serializeEntry(e.getClass.getCanonicalName,
                       e.timestamp,
                       e.db,
                       e.namespace,
                       e.metric,
                       extractValue(e.bit.value),
                       extractDimensions(e.bit.dimensions))
      case e: RejectEntry =>
        serializeEntry(e.getClass.getCanonicalName,
                       e.timestamp,
                       e.db,
                       e.namespace,
                       e.metric,
                       extractValue(e.bit.value),
                       extractDimensions(e.bit.dimensions))
      case e: DeleteEntry =>
        serializeDeleteByQuery(e.getClass.getCanonicalName, e.timestamp, e.db, e.namespace, e.metric, e.expression)
      case e: DeleteNamespaceEntry => serializeCommons(e.getClass.getCanonicalName, e.timestamp, e.db, e.namespace)
      case e: DeleteMetricEntry =>
        serializeDeleteMetric(e.getClass.getCanonicalName, e.timestamp, e.db, e.namespace, e.metric)
    }

  /**
    * Utils methods for common fields between [[CommitLogEntry]] concrete instances serialization
    *
    * @param className [[CommitLogEntry]] instance class canonicalName
    * @param ts timestamp
    * @param db database
    * @param namespace namespace
    * @return Bytes representation
    */
  private def serializeCommons(className: String, ts: Long, db: String, namespace: String): Array[Byte] = {
    writeBuffer.clear()
    //classname
    writeBuffer.write(className)
    // timestamp
    writeBuffer.write(ts.toString)
    //dbx
    writeBuffer.write(db)
    //namespace
    writeBuffer.write(namespace)

    writeBuffer.array
  }

  /**
    * Serializes a [[DeleteEntry]]
    *
    * @param className class canonicalName
    * @param ts timestamp
    * @param db database
    * @param namespace namespace
    * @param metric metric
    * @param expression delete query where condition
    * @return Bytes representation
    */
  private def serializeDeleteByQuery(
      className: String,
      ts: Long,
      db: String,
      namespace: String,
      metric: String,
      expression: Expression
  ): Array[Byte] = {
    serializeCommons(className, ts, db, namespace)
    // metric
    writeBuffer.write(metric)
    extractExpression(expression)
    writeBuffer.array

  }

  /**
    * Serializes a [[DeleteMetricEntry]]
    *
    * @param className class canonicalName
    * @param ts timestamp
    * @param db database
    * @param namespace namespace
    * @param metric metric
    * @return Bytes representation
    */
  private def serializeDeleteMetric(
      className: String,
      ts: Long,
      db: String,
      namespace: String,
      metric: String
  ): Array[Byte] = {
    serializeCommons(className, ts, db, namespace)
    writeBuffer.write(metric)
    writeBuffer.array
  }

  /**
    * Serializes [[InsertEntry]] and [[RejectEntry]]
    *
    * @param className class canonicalName
    * @param ts timestamp
    * @param db database
    * @param namespace namespace
    * @param metric metric
    * @param value [[Bit]] value
    * @param dimensions [[Bit]] dimensions
    * @return Bytes representation
    */
  private def serializeEntry(
      className: String,
      ts: Long,
      db: String,
      namespace: String,
      metric: String,
      value: Value,
      dimensions: List[Dimension]
  ): Array[Byte] = {
    serializeCommons(className, ts, db, namespace)
    // metric
    writeBuffer.write(metric)
    // dimensions
    writeBuffer.putInt(dimensions.length)
    dimensions.foreach {
      case (name, typ, value) =>
        writeBuffer.write(name)
        writeBuffer.write(typ)
        writeBuffer.putInt(value.length)
        writeBuffer.put(value)
    }

    writeBuffer.array
  }

}

abstract class BaseBuffer(maxSize: Int) {

  protected val buffer: ByteBuffer = ByteBuffer.allocate(maxSize)
}

private class WriteBuffer(maxSize: Int) extends BaseBuffer(maxSize) {

  def array: Array[Byte] = buffer.array

  def clear(): Buffer = buffer.clear()

  def put(v: Array[Byte]): ByteBuffer = buffer.put(v)

  def putInt(v: Int): ByteBuffer = buffer.putInt(v)

  def write(s: String): Unit = {
    val xs = s.getBytes
    putInt(xs.length)
    put(xs)
  }
}

private class ReadBuffer(maxSize: Int) extends BaseBuffer(maxSize) {

  def clear(array: Array[Byte]): Buffer = {
    buffer.clear()
    buffer.put(array)
    buffer.position(0)
  }

  def get(v: Array[Byte]): Unit = buffer.get(v)

  def getInt: Int = buffer.getInt

  def read: String = {
    val length = buffer.getInt
    val array  = new Array[Byte](length)
    buffer.get(array)
    new String(array)
  }
}
