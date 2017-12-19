package io.radicalbit.nsdb.index

import cats.data.Validated.{Invalid, Valid, invalidNel, valid}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import io.radicalbit.nsdb.validation.Validation.{FieldValidation, WriteValidation}
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

abstract class AbstractTimeSeriesIndex extends Index[Bit] with TypeSupport {

  override def _keyField: String = "timestamp"

  def write(data: Bit)(implicit writer: IndexWriter): WriteValidation = {
    val doc       = new Document
    val allFields = validateRecord(data)
    allFields match {
      case Valid(fields) =>
        fields.foreach(f => {
          doc.add(f)
        })
        Try(writer.addDocument(doc)) match {
          case Success(id) => valid(id)
          case Failure(ex) => invalidNel(ex.getMessage)
        }
      case errs @ Invalid(_) => errs
    }
  }

  override def validateRecord(bit: Bit): FieldValidation =
    validateSchemaTypeSupport(bit)
      .map(se => se.flatMap(elem => elem.indexType.indexField(elem.name, elem.indexType.cast(elem.value))))

  override def toRecord(document: Document, fields: Seq[SimpleField]): Bit = {
    val dimensions: Map[String, JSerializable] =
      document.getFields.asScala
        .filterNot(f =>
          f.name() == _keyField || f.name() == _valueField || f.name() == _countField || (fields.nonEmpty &&
            !fields.exists(sf => sf.name == f.name() && !sf.count)))
        .map {
          case f if f.numericValue() != null => f.name() -> f.numericValue()
          case f                             => f.name() -> f.stringValue()
        }
        .toMap
    val aggregated: Map[String, JSerializable] =
      fields.filter(_.count).map(_.toString -> document.getField("_count").numericValue()).toMap

    val value = document.getField(_valueField).numericValue()
    Bit(timestamp = document.getField(_keyField).numericValue().longValue(),
        value = value,
        dimensions = dimensions ++ aggregated)
  }

  def delete(data: Bit)(implicit writer: IndexWriter): Unit = {
    val query = LongPoint.newExactQuery(_keyField, data.timestamp)
    writer.deleteDocuments(query)
    writer.forceMergeDeletes(true)
  }
}
