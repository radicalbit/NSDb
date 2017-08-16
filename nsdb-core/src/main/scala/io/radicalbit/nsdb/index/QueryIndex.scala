package io.radicalbit.nsdb.index

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import cats.data.Validated.{Invalid, Valid, invalidNel, valid}
import io.radicalbit.nsdb.statement.SelectSQLStatement
import io.radicalbit.nsdb.validation.Validation.{FieldValidation, LongValidation}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, StoredField, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.lucene.store.BaseDirectory

import scala.util.{Failure, Success, Try}

case class NsdbQuery(uuid: String, query: SelectSQLStatement)

class QueryIndex(override val directory: BaseDirectory) extends Index[NsdbQuery, NsdbQuery] {
  override val _keyField: String = "_uuid"
  val queryField                 = "query"

  override def validateRecord(data: NsdbQuery): FieldValidation = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(data.query)
    val binary = b.toByteArray()
    b.close()
    o.close()
    valid(
      Seq(
        new StringField(_keyField, data.uuid.toLowerCase, Store.YES),
        new StoredField(queryField, binary)
      )
    )
  }

  override def write(data: NsdbQuery)(implicit writer: IndexWriter): LongValidation = {
    val doc = new Document
    validateRecord(data) match {
      case Valid(fields) =>
        Try {
          fields.foreach(doc.add)
          writer.addDocument(doc)
        } match {
          case Success(id) => valid(id)
          case Failure(ex) => invalidNel(ex.getMessage)
        }
      case errs @ Invalid(_) => errs
    }
  }

  override def toRecord(document: Document): NsdbQuery = {
    val binary    = document.getBinaryValue(queryField).bytes
    val b         = new ByteArrayInputStream(binary)
    val o         = new ObjectInputStream(b)
    val statement = o.readObject().asInstanceOf[SelectSQLStatement]
    o.close()
    b.close()

    NsdbQuery(
      document.get(_keyField),
      statement
    )
  }

  def getQuery(uuid: String): Option[NsdbQuery] = {
    Try(query(_keyField, uuid, 1).headOption) match {
      case Success(queryOpt) => queryOpt
      case Failure(_)        => None
    }
  }

//  def update(metric: String, newSchema: Schema)(implicit writer: IndexWriter): LongValidation = {
//    getSchema(metric) match {
//      case Some(oldSchema) =>
//        delete(oldSchema)
//        val newFields = oldSchema.fields.toSet ++ newSchema.fields.toSet
//        write(Schema(newSchema.metric, newFields.toSeq))
//      case None => write(newSchema)
//    }
//  }

  override def delete(data: NsdbQuery)(implicit writer: IndexWriter): Unit = {
    val parser   = new QueryParser(_keyField, new StandardAnalyzer())
    val query    = parser.parse(data.uuid)
    val reader   = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)
    val hits     = searcher.search(query, 1)
    (0 until hits.totalHits).foreach { _ =>
      writer.deleteDocuments(query)
    }
    writer.forceMergeDeletes(true)
  }
}

//object SchemaIndex {
//  def getCompatibleSchema(oldSchema: Schema, newSchema: Schema): Validated[NonEmptyList[String], Seq[SchemaField]] = {
//    val newFields = newSchema.fields.map(e => e.name -> e).toMap
//    val oldFields = oldSchema.fields.map(e => e.name -> e).toMap
//    oldSchema.fields
//      .map(oldField => {
//        if (newFields.get(oldField.name).isDefined && oldField.indexType != newFields(oldField.name).indexType)
//          invalidNel("")
//        else valid(Seq(newFields.getOrElse(oldField.name, oldFields(oldField.name))))
//      })
//      .toList
//      .combineAll
//      .map(oldFields => (oldFields.toSet ++ newFields.values.toSet).toSeq)
//  }
//}
