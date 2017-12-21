package io.radicalbit.nsdb.index

import cats.Monoid
import cats.data.Validated.{Invalid, Valid, invalidNel, valid}
import cats.data.{NonEmptyList, Validated}
import cats.implicits._
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.IndexType.SchemaValidation
import io.radicalbit.nsdb.model.{RawField, TypedField}
import io.radicalbit.nsdb.{JDouble, JLong}
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.util.BytesRef

import scala.util.{Failure, Success, Try}

trait TypeSupport {

  implicit val schemaValidationMonoid: Monoid[SchemaValidation] = new Monoid[SchemaValidation] {
    override val empty: SchemaValidation = valid(Seq.empty)

    override def combine(x: SchemaValidation, y: SchemaValidation): SchemaValidation =
      (x, y) match {
        case (Valid(a), Valid(b))       => valid(a ++ b)
        case (Valid(_), k @ Invalid(_)) => k
        case (f @ Invalid(_), Valid(_)) => f
        case (Invalid(l1), Invalid(l2)) => Invalid(l1.combine(l2))
      }
  }

  def validateSchemaTypeSupport(bit: Bit): SchemaValidation = {
    (bit.dimensions ++ Map("value" -> bit.value, "timestamp" -> bit.timestamp.asInstanceOf[JSerializable]))
      .map { case (n, v) => IndexType.fromRawField(RawField(n, v)) }
      .toList
      .combineAll
  }
}

sealed trait IndexType[T] {
  def actualType: Class[T]

  def indexField(fieldName: String, value: T): Seq[Field]

  def facetField(fieldName: String, value: T): Seq[Field]

  def serialize(value: T): Array[Byte] = value.toString.getBytes()

  def deserialize(value: Array[Byte]): T

  def cast[T](a: Any): T = a.asInstanceOf[T]

}

sealed trait NumericType[T] extends IndexType[T]

sealed trait StringType[T] extends IndexType[T]

object IndexType {

  type SchemaValidation = Validated[NonEmptyList[String], Seq[TypedField]]

  private val supportedType = Seq(INT(), BIGINT(), DECIMAL(), CHAR(), VARCHAR())

  def fromRawField(rawField: RawField): SchemaValidation =
    supportedType.find(_.actualType == rawField.value.getClass) match {
      case Some(indexType) => valid(Seq(TypedField(rawField.name, indexType, rawField.value)))
      case None            => invalidNel(s"class ${rawField.value.getClass} is not supported")
    }

  def fromClass(clazz: Class[_]): Try[IndexType[_]] = supportedType.find(_.actualType == clazz) match {
    case Some(indexType: IndexType[_]) => Success(indexType)
    case None                          => Failure(new RuntimeException(s"unsupported type $clazz"))
  }

}

case class INT() extends NumericType[Integer] {
  def actualType = classOf[Integer]
  override def indexField(fieldName: String, value: Integer): Seq[Field] =
    Seq(new IntPoint(fieldName, value.toString.toInt),
        new NumericDocValuesField(fieldName, value.toString.toLong),
        new StoredField(fieldName, value.toString.toInt))
  override def facetField(fieldName: String, value: Integer): Seq[Field] =
    Seq(
      new IntPoint(fieldName, value.toString.toInt),
      new NumericDocValuesField(fieldName, value.toString.toLong)
    )
  def deserialize(value: Array[Byte]) = new String(value).toInt
}
case class BIGINT() extends NumericType[JLong] {
  def actualType = classOf[JLong]
  override def indexField(fieldName: String, value: JLong): Seq[Field] =
    Seq(
      new LongPoint(fieldName, value.toString.toLong),
      new NumericDocValuesField(fieldName, value.toString.toLong),
      new StoredField(fieldName, value.toString.toLong)
    )
  override def facetField(fieldName: String, value: JLong): Seq[Field] =
    Seq(
      new LongPoint(fieldName, value.toString.toLong),
      new NumericDocValuesField(fieldName, value.toString.toLong)
    )
  def deserialize(value: Array[Byte]) = new String(value).toLong
}
case class DECIMAL() extends NumericType[JDouble] {
  def actualType = classOf[JDouble]
  override def indexField(fieldName: String, value: JDouble): Seq[Field] =
    Seq(
      new DoublePoint(fieldName, value.toString.toDouble),
      new DoubleDocValuesField(fieldName, value.toString.toDouble),
      new StoredField(fieldName, value.toString.toFloat)
    )
  override def facetField(fieldName: String, value: JDouble): Seq[Field] =
    Seq(
      new DoublePoint(fieldName, value.toString.toDouble),
      new DoubleDocValuesField(fieldName, value.toString.toDouble)
    )
  def deserialize(value: Array[Byte]) = new String(value).toDouble
}
case class BOOLEAN() extends IndexType[Boolean] {
  def actualType = classOf[Boolean]
  override def indexField(fieldName: String, value: Boolean): Seq[Field] =
    Seq(new StringField(fieldName, value.toString, Store.YES))
  override def facetField(fieldName: String, value: Boolean): Seq[Field] =
    Seq(new StringField(fieldName, value.toString, Store.YES))
  def deserialize(value: Array[Byte]) = new String(value).toBoolean
}
case class CHAR() extends StringType[Char] {
  def actualType = classOf[Char]
  override def indexField(fieldName: String, value: Char): Seq[Field] =
    Seq(new StringField(fieldName, value.toString, Store.YES))
  override def facetField(fieldName: String, value: Char): Seq[Field] =
    Seq(new StringField(fieldName, value.toString, Store.YES))

  def deserialize(value: Array[Byte]) = new String(value).charAt(0)
}
case class VARCHAR() extends StringType[String] {
  def actualType = classOf[String]
  override def indexField(fieldName: String, value: String): Seq[Field] =
    Seq(
      new StringField(fieldName, value, Store.YES),
      new SortedDocValuesField(fieldName, new BytesRef(value))
    )
  override def facetField(fieldName: String, value: String): Seq[Field] =
    Seq(
      new StringField(fieldName, value, Store.YES)
    )
  def deserialize(value: Array[Byte]) = new String(value)
}
