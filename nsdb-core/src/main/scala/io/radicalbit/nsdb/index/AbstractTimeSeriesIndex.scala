package io.radicalbit.nsdb.index

import cats.data.Validated.{Invalid, Valid, invalidNel, valid}
import io.radicalbit.nsdb.JLong
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.{Record, RecordOut}
import io.radicalbit.nsdb.validation.Validation.{FieldValidation, LongValidation, fieldSemigroup}
import org.apache.lucene.document._
import org.apache.lucene.index.{DirectoryReader, IndexWriter}
import org.apache.lucene.search.{IndexSearcher, Sort}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

abstract class AbstractTimeSeriesIndex extends Index[Record, RecordOut] with TypeSupport {

  val _lastRead          = "_lastRead"
  override val _keyField = "timestamp"

  def write(data: Record)(implicit writer: IndexWriter): LongValidation = {
    val doc     = new Document
    val curTime = System.currentTimeMillis
    val allFields = validateRecord(data).map(
      fields =>
        fields ++ Seq(
          new LongPoint(_keyField, data.timestamp),
          new StoredField(_keyField, data.timestamp),
          new NumericDocValuesField(_keyField, data.timestamp),
          new LongPoint(_lastRead, curTime),
          new StoredField(_lastRead, System.currentTimeMillis)
      ))
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

  override def validateRecord(data: Record): FieldValidation = {
    validateSchemaTypeSupport(data.dimensions)
      .map(se => se.flatMap(elem => elem.indexType.indexField(elem.name, elem.value)))
      .combine(
        validateSchemaTypeSupport(Map("value" -> data.metric)).map(se =>
          se.flatMap(elem => Seq(new StoredField(elem.name, elem.value.toString))))
      )
  }

  override def toRecord(document: Document): RecordOut = {
    val fields: Map[String, JSerializable] =
      document.getFields.asScala
        .filterNot(f => f.name() == _keyField || f.name() == _lastRead)
        .map {
          case f if f.numericValue() != null => f.name() -> new JLong(f.numericValue().longValue())
          case f                             => f.name() -> f.stringValue()
        }
        .toMap
    RecordOut(document.getField(_keyField).numericValue().longValue(), fields)
  }

  def delete(data: Record)(implicit writer: IndexWriter): Unit = {
    val query    = LongPoint.newRangeQuery(_keyField, data.timestamp, data.timestamp)
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    val hits     = searcher.search(query, 1)
    (0 until hits.totalHits).foreach { _ =>
      writer.deleteDocuments(query)
    }
    writer.forceMergeDeletes(true)
  }

  def timeRange(start: Long, end: Long, size: Int = Int.MaxValue, sort: Option[Sort] = None): Seq[RecordOut] = {
    val rangeQuery = LongPoint.newRangeQuery(_keyField, start, end)
    query(rangeQuery, size, sort)
  }

}
