package io.radicalbit.index

import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._

import scala.util.{Failure, Success, Try}

trait TypeSupport {

  private def sequence[T](xs: Seq[Try[T]]): Try[Seq[T]] = (Try(Seq[T]()) /: xs) { (a, b) =>
    a flatMap (c => b map (d => c :+ d))
  }

  def validateSchema(fields: Map[String, Any]): Try[Seq[IndexType[_]]] =
    sequence(fields.map { case (_, v) => IndexType.fromClass(v.getClass) }.toSeq)

  sealed trait IndexType[T] {
    def actualType: Class[T]
    def indexField(fieldName: String, value: Any): Seq[Field]
  }

  object IndexType {
    private val supportedType = Seq(TIMESTAMP(), INT(), BIGINT(), DECIMAL(), CHAR(), VARCHAR())

    def fromClass(clazz: Class[_]): Try[IndexType[_]] = supportedType.find(_.actualType == clazz) match {
      case Some(indexType: IndexType[_]) => Success(indexType)
      case None                          => Failure(new RuntimeException(s"unsupported type ${}"))
    }
  }

  case class TIMESTAMP() extends IndexType[Long] {
    def actualType = classOf[Long]
    override def indexField(fieldName: String, value: Any): Seq[Field] =
      Seq(new LongPoint(fieldName, value.toString.toLong), new StoredField(fieldName, value.toString.toLong))
  }
  case class INT() extends IndexType[Int] {
    def actualType = classOf[Int]
    override def indexField(fieldName: String, value: Any): Seq[Field] =
      Seq(new IntPoint(fieldName, value.toString.toInt), new StoredField(fieldName, value.toString.toInt))
  }
  case class BIGINT() extends IndexType[Long] {
    def actualType = classOf[Long]
    override def indexField(fieldName: String, value: Any): Seq[Field] =
      Seq(new LongPoint(fieldName, value.toString.toLong), new StoredField(fieldName, value.toString.toLong))
  }
  case class DECIMAL() extends IndexType[Float] {
    def actualType = classOf[Float]
    override def indexField(fieldName: String, value: Any): Seq[Field] =
      Seq(new FloatPoint(fieldName, value.toString.toFloat), new StoredField(fieldName, value.toString.toFloat))
  }
  case class BOOLEAN() extends IndexType[Boolean] {
    def actualType = classOf[Boolean]
    override def indexField(fieldName: String, value: Any): Seq[Field] =
      Seq(new StringField(fieldName, value.toString, Store.YES))
  }
  case class CHAR() extends IndexType[Char] {
    def actualType = classOf[Char]
    override def indexField(fieldName: String, value: Any): Seq[Field] =
      Seq(new StringField(fieldName, value.toString, Store.YES))
  }
  case class VARCHAR() extends IndexType[String] {
    def actualType = classOf[String]
    override def indexField(fieldName: String, value: Any): Seq[Field] =
      Seq(new StringField(fieldName, value.toString, Store.YES))
  }
}
