/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import io.radicalbit.nsdb.common._
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.radicalbit.nsdb.common.exception.TypeNotSupportedException
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.{RawField, TypedField}
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.util.BytesRef

import scala.util.{Failure, Success, Try}

/**
  * Contains a utility method to validate a bit.
  */
trait TypeSupport {

  /**
    * Checks if every field provided is valid.
    * @param bit the bit to check.
    * @return a map of [[TypedField]] keyed by field name. [[Failure]] if at least one field is invalid.
    */
  def validateSchemaTypeSupport(bit: Bit): Try[Map[String, TypedField]] = {
    val x = bit.fields
      .map { case (n, (v, t)) => n -> IndexType.fromRawField(RawField(n, t, v)) }
    Try(x.mapValues(f => f.get).map(identity))
  }
}

/**
  * Internal type system trait.
  * Direct children are
  *
  * - [[NumericType]] for numeric types
  *
  * @tparam T corresponding raw type.
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[BIGINT], name = "BIGINT"),
    new JsonSubTypes.Type(value = classOf[DECIMAL], name = "DECIMAL"),
    new JsonSubTypes.Type(value = classOf[INT], name = "INT"),
    new JsonSubTypes.Type(value = classOf[VARCHAR], name = "VARCHAR")
  ))
sealed trait IndexType[T] extends Serializable {

  /**
    * @return the scala type.
    */
  def actualType: Manifest[T]

  /**
    * @param fieldName field name
    * @param value field value.
    * @return the sequence of lucene [[Field]] to be added into indexes during write operations.
    */
  def indexField(fieldName: String, value: NSDbType): Seq[Field]

  /**
    * @param fieldName field name
    * @param value field value.
    * @return the sequence of lucene [[Field]] to be added into facet indexes during write operations.
    */
  def facetField(fieldName: String, value: NSDbType): Seq[Field]

  /**
    * @return scala [[Ordering]] for the type.
    */
  def ord: Ordering[Any]

  /**
    * Serializes a value of this type.
    * @param value the value.
    * @return the byte array.
    */
  def serialize(value: NSDbType): Array[Byte] = value.rawValue.toString.getBytes()

  /**
    * Deserializes a byte array into this type.
    * @param value the byte array to deserialize
    * @return an instance of this type.
    */
  def deserialize(value: Array[Byte]): NSDbType

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
  * @tparam T corresponding raw type.
  */
sealed abstract class NumericType[T] extends IndexType[T] {

  /**
    * Returns a [[Numeric]] to be used for arithmetic operations
    */
  def numeric: Numeric[Any]
}

object IndexType {

  private val supportedType = Seq(INT(), BIGINT(), DECIMAL(), VARCHAR())

  def fromRawField(rawField: RawField): Try[TypedField] =
    supportedType.find(_.actualType == rawField.value.runtimeManifest) match {
      case Some(indexType) => Success(TypedField(rawField.name, rawField.fieldClassType, indexType, rawField.value))
      case None =>
        Failure(
          new RuntimeException(
            s"class ${Manifest.classType(rawField.value.rawValue.getClass).runtimeClass} is not supported"))
    }

  def fromManifest(manifest: Manifest[_]): Try[IndexType[_]] =
    supportedType.find(_.actualType == manifest) match {
      case Some(indexType: IndexType[_]) => Success(indexType)
      case None                          => Failure(new TypeNotSupportedException(s"unsupported type $manifest"))
    }

}

case class INT() extends NumericType[Int] {
  def actualType = manifest[Int]
  def ord        = Ordering[Int].asInstanceOf[Ordering[Any]]
  override def indexField(fieldName: String, value: NSDbType): Seq[Field] =
    Seq(
      new IntPoint(fieldName, cast(value.rawValue)),
      new NumericDocValuesField(fieldName, cast(value.rawValue)),
      new StoredField(fieldName, cast(value.rawValue)),
      new SortedDocValuesField(s"${fieldName}_str", new BytesRef(s"${value.rawValue}_str"))
    )
  override def facetField(fieldName: String, value: NSDbType): Seq[Field] =
    Seq(
      new IntPoint(fieldName, cast(value.rawValue)),
      new NumericDocValuesField(fieldName, cast(value.rawValue))
    )

  override def deserialize(value: Array[Byte]): NSDbIntType = NSDbIntType(new String(value).toInt)

  override def numeric: Numeric[Any] = implicitly[Numeric[Int]].asInstanceOf[Numeric[Any]]

  override def cast(value: Any): Int = value.toString.toInt
}
case class BIGINT() extends NumericType[Long] {
  def actualType = manifest[Long]
  def ord        = Ordering[Long].asInstanceOf[Ordering[Any]]
  override def indexField(fieldName: String, value: NSDbType): Seq[Field] =
    Seq(
      new LongPoint(fieldName, cast(value.rawValue)),
      new NumericDocValuesField(fieldName, cast(value.rawValue)),
      new StoredField(fieldName, cast(value.rawValue)),
      new SortedDocValuesField(s"${fieldName}_str", new BytesRef(s"${value.rawValue}_str"))
    )
  override def facetField(fieldName: String, value: NSDbType): Seq[Field] =
    Seq(
      new LongPoint(fieldName, cast(value.rawValue)),
      new NumericDocValuesField(fieldName, cast(value.rawValue))
    )
  def deserialize(value: Array[Byte]): NSDbLongType = NSDbLongType(new String(value).toLong)

  override def numeric: Numeric[Any] = implicitly[Numeric[Long]].asInstanceOf[Numeric[Any]]

  override def cast(value: Any): Long = value.toString.toLong
}
case class DECIMAL() extends NumericType[Double] {
  def actualType = manifest[Double]
  def ord        = Ordering[Double].asInstanceOf[Ordering[Any]]
  override def indexField(fieldName: String, value: NSDbType): Seq[Field] =
    Seq(
      new DoublePoint(fieldName, cast(value.rawValue)),
      new DoubleDocValuesField(fieldName, cast(value.rawValue)),
      new StoredField(fieldName, cast(value.rawValue)),
      new SortedDocValuesField(s"${fieldName}_str", new BytesRef(s"${value.rawValue}_str"))
    )
  override def facetField(fieldName: String, value: NSDbType): Seq[Field] =
    Seq(
      new DoublePoint(fieldName, cast(value.rawValue)),
      new DoubleDocValuesField(fieldName, cast(value.rawValue))
    )
  def deserialize(value: Array[Byte]): NSDbDoubleType = NSDbDoubleType(new String(value).toDouble)

  override def numeric: Numeric[Any] = implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]

  override def cast(value: Any): Double = value.toString.toDouble
}
case class VARCHAR() extends IndexType[String] {
  def actualType = manifest[String]
  def ord        = Ordering[String].asInstanceOf[Ordering[Any]]
  override def indexField(fieldName: String, value: NSDbType): Seq[Field] =
    Seq(
      new StringField(fieldName, cast(value.rawValue), Store.YES),
      new SortedDocValuesField(fieldName, new BytesRef(cast(value.rawValue)))
    )
  override def facetField(fieldName: String, value: NSDbType): Seq[Field] =
    Seq(
      new StringField(fieldName, cast(value.rawValue), Store.YES),
      new SortedDocValuesField(fieldName, new BytesRef(cast(value.rawValue)))
    )
  def deserialize(value: Array[Byte]) = NSDbStringType(new String(value))

  override def cast(value: Any): String = value.toString
}
