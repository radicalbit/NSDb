package io.radicalbit.nsdb.index

import cats.Monoid
import cats.data.{NonEmptyList, Validated}
import io.radicalbit.nsdb.index.Index.{FieldValidation, LongValidation}
import io.radicalbit.nsdb.model.{Record, SchemaField}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, StringField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.lucene.store.BaseDirectory
import cats.data.Validated.{Invalid, Valid, invalidNel, valid}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import cats.implicits._

case class Schema(metric: String, fields: Seq[SchemaField])

object Schema extends TypeSupport {
  def apply(metric: String, record: Record): Validated[NonEmptyList[String], Schema] = {
    validateSchemaTypeSupport(record.dimensions ++ record.fields).map(fields =>
      Schema(metric, fields.map(field => SchemaField(field.name, field.indexType))))
  }
}

class SchemaIndex(override val directory: BaseDirectory) extends Index[Schema, Schema] {
  override val _keyField: String = "_metric"

  type SchemaValidation = Validated[NonEmptyList[String], Seq[SchemaField]]
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

  override protected def recordFields(data: Schema): FieldValidation = {
    valid(
      Seq(
        new StringField(_keyField, data.metric.toLowerCase, Store.YES)
      ) ++
        data.fields.map(e => new StringField(e.name, e.indexType.getClass.getCanonicalName, Store.YES)))
  }

  override def write(data: Schema)(implicit writer: IndexWriter): LongValidation = {
    val doc = new Document
    recordFields(data) match {
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

  def isCompatibleSchema(oldSchema: Schema, newSchema: Schema): Validated[NonEmptyList[String], Seq[SchemaField]] = {
    val newFields = newSchema.fields.map(e => e.name -> e).toMap

    oldSchema.fields
      .map(oldField => {
        if (newFields.get(oldField.name).isDefined && oldField.indexType != newFields(oldField.name).indexType)
          invalidNel("")
        else valid(Seq(newFields(oldField.name)))
      })
      .toList
      .combineAll

//    val typeChecks = oldSchema.fields.map(e => newFields.get(e.name).isEmpty || newFields(e.name) == e._2)
//    oldSchema.metric == oldSchema.metric && typeChecks.reduce(_ && _)
  }

  override def docConversion(document: Document): Schema = {
    val fields = document.getFields.asScala.filterNot(_.name() == _keyField)
    Schema(
      document.get(_keyField),
      fields.map(f => SchemaField(f.name(), Class.forName(f.stringValue()).newInstance().asInstanceOf[IndexType[_]])))
  }

  def getAllSchemas: Seq[Schema] = {
    Try { query(new MatchAllDocsQuery(), Int.MaxValue, None) } match {
      case Success(docs: Seq[Schema]) =>
        docs
      case Failure(_) => Seq.empty
    }
  }

  def getSchema(metric: String): Option[Schema] = {
    query(_keyField, metric, 1).headOption
//        docs.headOption
//      case Failure(_) => None
//    }
  }

  def update(metric: String, newSchema: Schema)(implicit writer: IndexWriter): LongValidation = {
    getSchema(metric) match {
      case Some(oldSchema) =>
        delete(oldSchema)
        val newFields = oldSchema.fields.toSet ++ newSchema.fields.toSet
        write(Schema(newSchema.metric, newFields.toSeq))
      case None => write(newSchema)
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
