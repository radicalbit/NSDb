package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.{SchemaField, TypedField}
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.index.{IndexWriter, Term}
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, TermQuery}
import org.apache.lucene.store.BaseDirectory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class Schema(metric: String, fields: Set[SchemaField]) {
  override def equals(obj: scala.Any): Boolean = {
    if (obj != null && obj.isInstanceOf[Schema]) {
      val otherSchema = obj.asInstanceOf[Schema]
      (otherSchema.metric == this.metric) && (otherSchema.fields.size == this.fields.size) && (otherSchema.fields == this.fields)
    } else false
  }

  def fieldsMap: Map[String, SchemaField] =
    fields.map(f => f.name -> f).toMap
}

object Schema extends TypeSupport {
  def apply(metric: String, bit: Bit): Try[Schema] = {
    validateSchemaTypeSupport(bit).map((fields: Seq[TypedField]) =>
      Schema(metric, fields.map(field => SchemaField(field.name, field.indexType)).toSet))
  }
}

class SchemaIndex(override val directory: BaseDirectory) extends Index[Schema] {
  override val _keyField: String = "_metric"

  override def getSearcher: IndexSearcher = {
    refresh()
    super.getSearcher
  }

  override def validateRecord(data: Schema): Try[Seq[Field]] = {
    Success(
      Seq(
        new StringField(_keyField, data.metric.toLowerCase, Store.YES)
      ) ++
        data.fields.map(e => new StringField(e.name, e.indexType.getClass.getCanonicalName, Store.YES)))
  }

  override def write(data: Schema)(implicit writer: IndexWriter): Try[Long] = {
    val doc = new Document
    validateRecord(data) match {
      case Success(fields) =>
        Try {
          fields.foreach(doc.add)
          writer.addDocument(doc)
        }
      case Failure(t) => Failure(t)
    }
  }

  override def toRecord(document: Document, fields: Seq[SimpleField]): Schema = {
    val fields = document.getFields.asScala.filterNot(f => f.name() == _keyField || f.name() == _countField)
    Schema(document.get(_keyField),
           fields
             .map(f => SchemaField(f.name(), Class.forName(f.stringValue()).newInstance().asInstanceOf[IndexType[_]]))
             .toSet)
  }

  def getAllSchemas(implicit searcher: IndexSearcher): Seq[Schema] = {
    Try { query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None) } match {
      case Success(docs: Seq[Schema]) => docs
      case Failure(_)                 => Seq.empty
    }
  }

  def getSchema(metric: String): Option[Schema] = {
    Try(query(_keyField, metric, Seq.empty, 1).headOption) match {
      case Success(schemaOpt) => schemaOpt
      case Failure(_)         => None
    }
  }

  def update(metric: String, newSchema: Schema)(implicit writer: IndexWriter): Try[Long] = {
    getSchema(metric) match {
      case Some(oldSchema) =>
        delete(oldSchema)
        val newFields = oldSchema.fields ++ newSchema.fields
        write(Schema(newSchema.metric, newFields))
      case None => write(newSchema)
    }
  }

  override def delete(data: Schema)(implicit writer: IndexWriter): Unit = {
    val query = new TermQuery(new Term(_keyField, data.metric.toLowerCase))
    writer.deleteDocuments(query)
    writer.forceMergeDeletes(true)
  }
}

object SchemaIndex {
  def getCompatibleSchema(oldSchema: Schema, newSchema: Schema): Try[Set[SchemaField]] = {
    val newFields = newSchema.fields.map(e => e.name -> e).toMap
    val oldFields = oldSchema.fields.map(e => e.name -> e).toMap
    val checked = oldSchema.fields
      .map { oldField =>
        val newField = newFields.get(oldField.name)
        if (newField.isDefined && oldField.indexType != newField.get.indexType)
          Failure(new RuntimeException(
            s"mismatch type for field $oldField : new type is ${newField.get.indexType} while old type is ${oldField.indexType}"))
        else Success(newFields.getOrElse(oldField.name, oldFields(oldField.name)))
      }

    Try(checked.map(_.get)) match {
      case Success(_) => Success(oldSchema.fields ++ newSchema.fields)
      case Failure(t) => Failure(t)
    }
  }
}
