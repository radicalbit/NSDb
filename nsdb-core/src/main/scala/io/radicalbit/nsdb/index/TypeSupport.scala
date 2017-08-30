package io.radicalbit.nsdb.index

import cats.Monoid
import cats.data.{NonEmptyList, Validated}
import io.radicalbit.nsdb.JLong
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import cats.data.Validated.{Invalid, Valid, invalidNel, valid}
import cats.implicits._
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.index.IndexType.SchemaValidation
import io.radicalbit.nsdb.model.{RawField, TypedField}
import org.apache.lucene.util.BytesRef

import scala.util.{Failure, Success, Try}

trait TypeSupport {

  implicit val schemaValidationMonoid = new Monoid[SchemaValidation] {
    override val empty: SchemaValidation = valid(Seq.empty)

    override def combine(x: SchemaValidation, y: SchemaValidation): SchemaValidation =
      (x, y) match {
        case (Valid(a), Valid(b))       => valid(a ++ b)
        case (Valid(_), k @ Invalid(_)) => k
        case (f @ Invalid(_), Valid(_)) => f
        case (Invalid(l1), Invalid(l2)) => Invalid(l1.combine(l2))
      }
  }

  def validateSchemaTypeSupport(fields: Map[String, JSerializable]): SchemaValidation = {
    fields.map { case (n, v) => IndexType.fromRawField(RawField(n, v)) }.toList.combineAll
  }
}

sealed trait IndexType[T] {
  def actualType: Class[T]

  def indexField(fieldName: String, value: JSerializable): Seq[Field]

  def serialize(value: JSerializable): Array[Byte] = value.toString.getBytes()

  def deserialize(value: Array[Byte]): JSerializable

}

object IndexType {

  type SchemaValidation = Validated[NonEmptyList[String], Seq[TypedField]]

  private val supportedType = Seq(TIMESTAMP(), INT(), BIGINT(), DECIMAL(), CHAR(), VARCHAR())

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

case class TIMESTAMP() extends IndexType[Long] {
  def actualType = classOf[Long]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new LongPoint(fieldName, value.toString.toLong), new NumericDocValuesField(fieldName, value.toString.toLong), new StoredField(fieldName, value.toString.toLong))
  def deserialize(value: Array[Byte]) = new String(value).toLong
}
case class INT() extends IndexType[Integer] {
  def actualType = classOf[Integer]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new IntPoint(fieldName, value.toString.toInt), new NumericDocValuesField(fieldName, value.toString.toLong), new StoredField(fieldName, value.toString.toInt))
  def deserialize(value: Array[Byte]) = new String(value).toInt
}
case class BIGINT() extends IndexType[JLong] {
  def actualType = classOf[JLong]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new LongPoint(fieldName, value.toString.toLong), new NumericDocValuesField(fieldName, value.toString.toLong), new StoredField(fieldName, value.toString.toLong))
  def deserialize(value: Array[Byte]) = new String(value).toLong
}
case class DECIMAL() extends IndexType[Float] {
  def actualType = classOf[Float]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new FloatPoint(fieldName, value.toString.toFloat), new StoredField(fieldName, value.toString.toFloat))
  def deserialize(value: Array[Byte]) = new String(value).toFloat
}
case class BOOLEAN() extends IndexType[Boolean] {
  def actualType = classOf[Boolean]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new StringField(fieldName, value.toString, Store.YES))
  def deserialize(value: Array[Byte]) = new String(value).toBoolean
}
case class CHAR() extends IndexType[Char] {
  def actualType = classOf[Char]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new StringField(fieldName, value.toString, Store.YES))
  def deserialize(value: Array[Byte]) = new String(value).charAt(0)
}
case class VARCHAR() extends IndexType[String] {
  def actualType = classOf[String]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new StringField(fieldName, value.toString, Store.YES),
        new SortedDocValuesField(fieldName, new BytesRef(value.toString)))
  def deserialize(value: Array[Byte]) = new String(value)
}
