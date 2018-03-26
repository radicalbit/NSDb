package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

abstract class AbstractTimeSeriesIndex extends Index[Bit] with TypeSupport {

  override def _keyField: String = "timestamp"

  def write(data: Bit)(implicit writer: IndexWriter): Try[Long] = {
    val doc       = new Document
    val allFields = validateRecord(data)
    allFields match {
      case Success(fields) =>
        fields.foreach(f => {
          doc.add(f)
        })
        Try(writer.addDocument(doc))
      case Failure(t) => Failure(t)
    }
  }

  override def validateRecord(bit: Bit): Try[Seq[Field]] =
    validateSchemaTypeSupport(bit)
      .map(se => se.flatMap(elem => elem.indexType.indexField(elem.name, elem.value)))

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

  def delete(data: Bit)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val query  = LongPoint.newExactQuery(_keyField, data.timestamp)
      val result = writer.deleteDocuments(query)
      writer.forceMergeDeletes(true)
      result
    }
  }
}
