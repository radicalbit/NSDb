package io.radicalbit.index

import io.radicalbit.model.Record
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.BaseDirectory

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class Schema(metric: String, fields: Seq[(String, String)])

object Schema extends TypeSupport {
  def apply(metric: String, record: Record): Try[Schema] = {
    val typeConversions = record.dimensions.map { case (k, v) => (k, IndexType.fromClass(v.getClass)) }.toSeq ++ record.fields.map {
      case (k, v) => (k, IndexType.fromClass(v.getClass))
    }.toSeq
    validateSchema(record.dimensions ++ record.fields) match {
      case Success(_) =>
        Success(Schema(metric, typeConversions.map { case (k, v) => (k, v.get.getClass.getSimpleName) }))
      case Failure(ex) => Failure(ex)
    }
  }
}

class SchemaIndex(override val directory: BaseDirectory) extends Index[Schema] {
  override val _keyField: String = "_metric"

  override protected def writeRecord(doc: Document, data: Schema): Try[Document] = {
    doc.add(new StringField(_keyField, data.metric.toLowerCase, Store.YES))
    data.fields.foreach {
      case (f, t) =>
        doc.add(new StringField(f, Seq(f, t).mkString("|"), Store.YES))
    }
    Success(doc)
  }

  override def write(data: Schema)(implicit writer: IndexWriter): Try[Long] = {
    val doc = new Document
    writeRecord(doc, data)
    Try { writer.addDocument(doc) }
  }

  def isValidSchema(oldSchema: Schema, newSchema: Schema): Boolean = {
    val newFields  = newSchema.fields.toMap
    val typeChecks = oldSchema.fields.map(e => newFields.get(e._1).isEmpty || newFields(e._1) == e._2)
    oldSchema.metric == oldSchema.metric && typeChecks.reduce(_ && _)
  }

  def getSchema(metric: String): Option[Schema] = {
    Try(query(_keyField, metric, 1)) match {
      case Success(docs: Seq[Document]) =>
        docs.headOption.map(doc => {
          val fields = doc.getFields.asScala.filterNot(_.name() == _keyField).map(_.stringValue())
          Schema(doc.get(_keyField), fields.map(f => (f.split("\\|")(0), f.split("\\|")(1))))
        })
      case Failure(ex) =>
        println(ex)
        None
    }
  }

  def update(metric: String, newSchema: Schema)(implicit writer: IndexWriter): Try[Long] = {
    getSchema(metric) match {
      case Some(oldSchema) if isValidSchema(oldSchema, newSchema) =>
        delete(oldSchema)
        val newFields = oldSchema.fields.toSet ++ newSchema.fields.toSet
        write(Schema(newSchema.metric, newFields.toSeq))
      case Some(_) => Failure(new RuntimeException("incompatible schemas"))
      case None    => write(newSchema)
    }
  }

  override def delete(data: Schema)(implicit writer: IndexWriter): Unit = {
    val parser   = new QueryParser(_keyField, new StandardAnalyzer())
    val query    = parser.parse(data.metric)
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    val hits     = searcher.search(query, 1)
    (0 until hits.totalHits).foreach { _ =>
      writer.deleteDocuments(query)
    }
    writer.forceMergeDeletes(true)
  }
}
