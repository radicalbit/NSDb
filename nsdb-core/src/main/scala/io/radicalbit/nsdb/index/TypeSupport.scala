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

package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.common.exception.TypeNotSupportedException
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.{JDouble, JLong, JSerializable}
import io.radicalbit.nsdb.model.{RawField, TypedField}
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.util.BytesRef

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
  * Contains a utility method to validate a bit.
  */
trait TypeSupport {

  /**
    * Checks if every field provided is valid.
    * @param bit the bit to check.
    * @return a sequence of [[TypedField]]. [[Failure]] if at least one field is invalid.
    */
  def validateSchemaTypeSupport(bit: Bit): Try[Seq[TypedField]] = {
    val x = bit.fields.toSeq
      .map { case (n, v) => IndexType.tryFromRawField(RawField(n, v)) }
    Try(x.map(f => f.get))
  }
}

/**
  * Internal type system trait.
  * Direct children are
  *
  * - [[NumericType]] for numeric types
  *
  * - [[StringType]] for strings
  *
  * @tparam T corresponding java type.
  */
sealed trait IndexType[T] {

  /**
    * @return the scala type.
    */
  def actualType: Class[T]

  /**
    * @param fieldName field name
    * @param value field value.
    * @return the sequence of lucene [[Field]] to be added into indexes during write operations.
    */
  def indexField(fieldName: String, value: JSerializable): Seq[Field]

  /**
    * @param fieldName field name
    * @param value field value.
    * @return the sequence of lucene [[Field]] to be added into facet indexes during write operations.
    */
  def facetField(fieldName: String, value: JSerializable): Seq[Field]

  /**
    * @return scala [[Ordering]] for the type.
    */
  def ord: Ordering[JSerializable]

  /**
    * Serializes a value of this type.
    * @param value the value.
    * @return the byte array.
    */
  def serialize(value: JSerializable): Array[Byte] = value.toString.getBytes()

  /**
    * Deserializes a byte array into this type.
    * @param value the byte array to deserialize
    * @return an instance of this type.
    */
  def deserialize(value: Array[Byte]): T

  /**
    * Casts a [[Any]] into this type.
    * @param value generic value.
    * @return an instance of this type
    */
  def cast(value: Any): T

}

/**
  * Model for numeric types.
  * Subclasses are:
  *
  * - [[BIGINT]] for 64bit int numbers.
  *
  * - [[INT]] for 32bit int numbers.
  *
  * - [[DECIMAL]] for 64 bit floating point numbers.
  *
  * @tparam T corresponding java type.
  * @tparam ST corresponding scala type with [[Numeric]] context bound.
  */
sealed abstract class NumericType[T <: JSerializable, ST: Numeric: ClassTag] extends IndexType[T] {
  lazy val scalaNumeric = implicitly[Numeric[ST]]

  /**
    * Returns a [[Numeric]] to be used for arithmetic operations
    */
  def numeric: Numeric[JSerializable]
}

/**
  * Model for string type.
  * The only subclass is [[VARCHAR]].
  * @tparam T corresponding java type.
  */
sealed trait StringType[T <: JSerializable] extends IndexType[T]

object IndexType {

  private val supportedType = Seq(INT(), BIGINT(), DECIMAL(), VARCHAR())

  def fromRawField(rawField: RawField): Try[TypedField] =
    supportedType.find(_.actualType == rawField.value.getClass) match {
      case Some(indexType) => Success(TypedField(rawField.name, indexType, rawField.value))
      case None            => Failure(new RuntimeException(s"class ${rawField.value.getClass} is not supported"))
    }

  def tryFromRawField(rawField: RawField): Try[TypedField] =
    supportedType.find(_.actualType == rawField.value.getClass) match {
      case Some(indexType) => Success(TypedField(rawField.name, indexType, rawField.value))
      case None            => Failure(new RuntimeException(s"class ${rawField.value.getClass} is not supported"))
    }

  def fromClass(clazz: Class[_]): Try[IndexType[_]] = supportedType.find(_.actualType == clazz) match {
    case Some(indexType: IndexType[_]) => Success(indexType)
    case None                          => Failure(new TypeNotSupportedException(s"unsupported type $clazz"))
  }

}

case class INT() extends NumericType[Integer, Int] {
  def actualType = classOf[Integer]
  def ord        = Ordering[Integer].asInstanceOf[Ordering[JSerializable]]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new IntPoint(fieldName, value.toString.toInt),
        new NumericDocValuesField(fieldName, value.toString.toLong),
        new StoredField(fieldName, value.toString.toInt))
  override def facetField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(
      new IntPoint(fieldName, value.toString.toInt),
      new NumericDocValuesField(fieldName, value.toString.toLong)
    )
  def deserialize(value: Array[Byte]) = new String(value).toInt

  override def numeric: Numeric[JSerializable] = implicitly[Numeric[Int]].asInstanceOf[Numeric[JSerializable]]

  override def cast(value: Any): Integer = value.toString.toInt
}
case class BIGINT() extends NumericType[JLong, Long] {
  def actualType = classOf[JLong]
  def ord        = Ordering[JLong].asInstanceOf[Ordering[JSerializable]]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(
      new LongPoint(fieldName, value.toString.toLong),
      new NumericDocValuesField(fieldName, value.toString.toLong),
      new StoredField(fieldName, value.toString.toLong)
    )
  override def facetField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(
      new LongPoint(fieldName, value.toString.toLong),
      new NumericDocValuesField(fieldName, value.toString.toLong)
    )
  def deserialize(value: Array[Byte]) = new String(value).toLong

  override def numeric: Numeric[JSerializable] = implicitly[Numeric[Long]].asInstanceOf[Numeric[JSerializable]]

  override def cast(value: Any): JLong = value.toString.toLong
}
case class DECIMAL() extends NumericType[JDouble, Double] {
  def actualType = classOf[JDouble]
  def ord        = Ordering[JDouble].asInstanceOf[Ordering[JSerializable]]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(
      new DoublePoint(fieldName, value.toString.toDouble),
      new DoubleDocValuesField(fieldName, value.toString.toDouble),
      new StoredField(fieldName, value.toString.toDouble)
    )
  override def facetField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(
      new DoublePoint(fieldName, value.toString.toDouble),
      new DoubleDocValuesField(fieldName, value.toString.toDouble)
    )
  def deserialize(value: Array[Byte]) = new String(value).toDouble

  override def numeric: Numeric[JSerializable] = implicitly[Numeric[Double]].asInstanceOf[Numeric[JSerializable]]

  override def cast(value: Any): JDouble = value.toString.toDouble
}
case class VARCHAR() extends StringType[String] {
  def actualType = classOf[String]
  def ord        = Ordering[String].asInstanceOf[Ordering[JSerializable]]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(
      new StringField(fieldName, value.toString, Store.YES),
      new SortedDocValuesField(fieldName, new BytesRef(value.toString))
    )
  override def facetField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(
      new StringField(fieldName, value.toString, Store.YES),
      new SortedDocValuesField(fieldName, new BytesRef(value.toString))
    )
  def deserialize(value: Array[Byte]) = new String(value)

  override def cast(value: Any): String = value.toString
}
