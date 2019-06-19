/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.lucene.index.{IndexWriter, Term}
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.Directory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Index for entry of class [[Schema]].
  * @param directory index base directory.
  */
class SchemaIndex(override val directory: Directory) extends SimpleIndex[Schema] {

  import SchemaIndex._

  override val _keyField: String = "metric"

  override def validateRecord(data: Schema): Try[Seq[Field]] = {
    Success(
      Seq(
        new StringField(_keyField, data.metric, Store.YES)
      ) ++
        data.fields.map(e => new StringField(e.name, stringFieldValue(e), Store.YES)))
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
    Schema(
      document.get(_keyField),
      fields.map { f =>
        val (fieldType, indexType) = fieldValue(f.stringValue)
        SchemaField(f.name(), fieldType, indexType)
      }.toSet
    )
  }

  def getSchema(metric: String): Option[Schema] = {
    Try(query(_keyField, metric, Seq.empty, 1)(identity).headOption) match {
      case Success(schemaOpt) => schemaOpt
      case Failure(_)         => None
    }
  }

  def update(metric: String, newSchema: Schema)(implicit writer: IndexWriter): Try[Long] = {
    getSchema(metric) match {
      case Some(oldSchema) =>
        delete(oldSchema)
        write(newSchema)
      case None => write(newSchema)
    }
  }

  override def delete(data: Schema)(implicit writer: IndexWriter): Try[Long] =
    deleteMetricSchema(data.metric)

  def deleteMetricSchema(metric: String)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val query  = new TermQuery(new Term(_keyField, metric))
      val result = writer.deleteDocuments(query)
      writer.forceMergeDeletes(true)
      result
    }
  }
}

object SchemaIndex {

  /**
    * Assemblies, if possible, the union schema from 2 given schemas.
    * Given 2 schemas, they are compatible if fields present in both of them are of the same types.
    * The union schema is a schema with the union of the dimension sets.
    * @param firstSchema the first schema.
    * @param secondSchema the second schema.
    * @return the union schema.
    */
  def union(firstSchema: Schema, secondSchema: Schema): Try[Schema] = {
    val oldFields = firstSchema.fields.map(e => e.name -> e).toMap

    val notCompatibleFields = secondSchema.fields.collect {
      case field if oldFields.get(field.name).isDefined && oldFields(field.name).indexType != field.indexType =>
        s"mismatch type for field ${field.name} : new type ${field.indexType} is incompatible with old type"
    }

    if (notCompatibleFields.nonEmpty)
      Failure(new RuntimeException(notCompatibleFields.mkString(",")))
    else {
      val schema = Schema(secondSchema.metric, firstSchema.fields ++ secondSchema.fields)
      Success(schema)
    }
  }

  def stringFieldValue(sf: SchemaField): String = s"${sf.fieldClassType}-${sf.indexType.getClass.getCanonicalName}"

  def fieldValue(fieldSchemaType: String): (FieldClassType, IndexType[_]) = {
    val array = fieldSchemaType.split("-")
    val fieldType = array(0) match {
      case "TimestampFieldType" => TimestampFieldType
      case "ValueFieldType"     => ValueFieldType
      case "DimensionFieldType" => DimensionFieldType
      case "TagFieldType"       => TagFieldType
    }
    (fieldType, Class.forName(array(1)).newInstance().asInstanceOf[IndexType[_]])
  }
}
