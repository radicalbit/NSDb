package io.radicalbit.nsdb.index

import cats.data.Validated.{Invalid, Valid, invalidNel, valid}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.validation.Validation.{FieldValidation, WriteValidation}
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

abstract class AbstractTimeSeriesIndex extends Index[Bit] with TypeSupport {

  override val _keyField  = "timestamp"
  private val _valueField = "value"

  def write(data: Bit)(implicit writer: IndexWriter): WriteValidation = {
    val doc       = new Document
    val allFields = validateRecord(data)
    allFields match {
      case Valid(fields) =>
        fields.foreach(doc.add)
        Try(writer.addDocument(doc)) match {
          case Success(id) => valid(id)
          case Failure(ex) => invalidNel(ex.getMessage)
        }
      case errs @ Invalid(_) => errs
    }
  }

  override def validateRecord(bit: Bit): FieldValidation = {
    validateSchemaTypeSupport(bit)
      .map(se => se.flatMap(elem => elem.indexType.indexField(elem.name, elem.indexType.cast(elem.value))))
      .map(fields =>
        fields ++ Seq(
          new StoredField(_valueField, bit.value.toString),
          new LongPoint(_keyField, bit.timestamp),
          new StoredField(_keyField, bit.timestamp),
          new NumericDocValuesField(_keyField, bit.timestamp)
      ))
  }

  override def toRecord(document: Document, fields: Seq[String]): Bit = {
    val dimensions: Map[String, JSerializable] =
      document.getFields.asScala
        .filterNot(f =>
          f.name() == _keyField || f.name() == _valueField || (fields.nonEmpty && !fields
            .contains(f.name())))
        .map {
          case f if f.numericValue() != null => f.name() -> f.numericValue()
          case f                             => f.name() -> f.stringValue()
        }
        .toMap
    val value = document.getField(_valueField).numericValue()
    Bit(timestamp = document.getField(_keyField).numericValue().longValue(), value = value, dimensions = dimensions)
  }

  def delete(data: Bit)(implicit writer: IndexWriter): Unit = {
    val query = LongPoint.newExactQuery(_keyField, data.timestamp)
    writer.deleteDocuments(query)
    writer.forceMergeDeletes(true)
  }
}
