package io.radicalbit.nsdb.index

import io.radicalbit.{JLong, JSerializable}
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._

import scala.util.{Failure, Success, Try}

trait TypeSupport {

  private def sequence[T](xs: Seq[Try[T]]): Try[Seq[T]] = (Try(Seq[T]()) /: xs) { (a, b) =>
    a flatMap (c => b map (d => c :+ d))
  }

  def validateSchema(fields: Map[String, JSerializable]): Try[Seq[IndexType[_]]] =
    sequence(fields.map { case (_, v) => IndexType.fromClass(v.getClass) }.toSeq)
}

sealed trait IndexType[T] {
  def actualType: Class[T]

  def indexField(fieldName: String, value: JSerializable): Seq[Field]

  def serialize(value: JSerializable): Array[Byte] = value.toString.getBytes()

  def deserialize(value: Array[Byte]): JSerializable

}

object IndexType {
  private val supportedType = Seq(TIMESTAMP(), INT(), BIGINT(), DECIMAL(), CHAR(), VARCHAR())

  def fromClass(clazz: Class[_]): Try[IndexType[_]] = supportedType.find(_.actualType == clazz) match {
    case Some(indexType: IndexType[_]) => Success(indexType)
    case None                          => Failure(new RuntimeException(s"unsupported type $clazz"))
  }
}

case class TIMESTAMP() extends IndexType[Long] {
  def actualType = classOf[Long]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new LongPoint(fieldName, value.toString.toLong), new StoredField(fieldName, value.toString.toLong))
  def deserialize(value: Array[Byte]) = new String(value).toLong
}
case class INT() extends IndexType[Integer] {
  def actualType = classOf[Integer]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new IntPoint(fieldName, value.toString.toInt), new StoredField(fieldName, value.toString.toInt))
  def deserialize(value: Array[Byte]) = new String(value).toInt
}
case class BIGINT() extends IndexType[JLong] {
  def actualType = classOf[JLong]
  override def indexField(fieldName: String, value: JSerializable): Seq[Field] =
    Seq(new LongPoint(fieldName, value.toString.toLong), new StoredField(fieldName, value.toString.toLong))
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
    Seq(new StringField(fieldName, value.toString, Store.YES))
  def deserialize(value: Array[Byte]) = new String(value)
}
