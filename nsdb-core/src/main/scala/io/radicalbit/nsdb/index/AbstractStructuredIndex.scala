/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType, FieldClassType, TagFieldType}
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.index.lucene.Index
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.apache.lucene.search.grouping.{AllGroupHeadsCollector, TermGroupSelector}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Generic Time series index based on entries of class [[Bit]].
  */
abstract class AbstractStructuredIndex extends Index[Bit] with TypeSupport {

  override def _keyField: String = "timestamp"

  override def write(data: Bit)(implicit writer: IndexWriter): Try[Long] = {
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

  override def validateRecord(bit: Bit): Try[Iterable[Field]] =
    validateSchemaTypeSupport(bit)
      .map(se => se.flatMap { case (_, elem) => elem.indexType.indexField(elem.name, elem.value) })

  override def delete(data: Bit)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val query  = LongPoint.newExactQuery(_keyField, data.timestamp)
      val result = writer.deleteDocuments(query)
      writer.forceMergeDeletes(true)
      result
    }
  }

  def toRecord(schema: Schema, document: Document, fields: Seq[SimpleField]): Bit = {

    def extractFields(schema: Schema, document: Document, fields: Seq[SimpleField], fieldClassType: FieldClassType) = {
      document.getFields.asScala
        .filterNot { f =>
          schema.fieldsMap
            .find {
              case (_, fieldSchema) => fieldSchema.name == f.name && fieldSchema.fieldClassType == fieldClassType
            }
            .forall { _ =>
              f.name() == _keyField || f.name() == _valueField || f.name() == _countField || (fields.nonEmpty &&
              !fields.exists { sf =>
                (sf.name == f.name() || sf.name.trim == "*") && !sf.count
              })
            }
        }
        .map {
          case f if f.numericValue() != null => f.name() -> NSDbType(f.numericValue())
          case f                             => f.name() -> NSDbType(f.stringValue())
        }
    }

    val dimensions: Map[String, NSDbType] = extractFields(schema, document, fields, DimensionFieldType).toMap
    val tags: Map[String, NSDbType]       = extractFields(schema, document, fields, TagFieldType).toMap

    val aggregated: Map[String, NSDbType] =
      fields.filter(_.count).map(_.toString -> NSDbType(document.getField("_count").numericValue())).toMap

    val value = document.getField(_valueField).numericValue()
    Bit(
      timestamp = document.getField(_keyField).numericValue().longValue(),
      value = NSDbNumericType(value),
      dimensions = dimensions,
      tags = tags ++ aggregated
    )
  }

  private[index] def rawQuery(query: Query, limit: Int, sort: Option[Sort])(
      implicit searcher: IndexSearcher): Seq[Document] = {
    executeQuery(searcher, query, limit, sort)(identity)
  }

  /**
    * Executes a simple [[Query]] using the given schema.
    * @param schema the [[Schema]] to be used.
    * @param query the [[Query]] to be executed.
    * @param fields sequence of fields that must be included in the result.
    * @param limit results limit.
    * @param sort optional lucene [[Sort]].
    * @param f function to obtain an element B from an element T.
    * @tparam B return type.
    * @return the query results as a list of entries.
    */
  def query[B](schema: Schema, query: Query, fields: Seq[SimpleField], limit: Int, sort: Option[Sort])(
      f: Bit => B): Seq[B] = {
    if (fields.nonEmpty && fields.forall(_.count)) {
      executeCountQuery(this.getSearcher, query, limit) { doc =>
        f(toRecord(schema, doc, fields))
      }
    } else
      executeQuery(this.getSearcher, query, limit, sort) { doc =>
        f(toRecord(schema: Schema, doc, fields))
      }
  }

  /**
    * Returns all the entries where `field` = `value`
    * @param field the field name to use to filter data.
    * @param value the value to check the field with.
    * @param fields sequence of fields that must be included in the result.
    * @param limit results limit.
    * @param sort optional lucene [[Sort]].
    * @param f function to obtain an element B from an element T.
    * @tparam B return type.
    * @return the manipulated Seq.
    */
  def query[B](schema: Schema,
               field: String,
               value: String,
               fields: Seq[SimpleField],
               limit: Int,
               sort: Option[Sort] = None)(f: Bit => B): Seq[B] = {
    val parser = new QueryParser(field, new StandardAnalyzer())
    val q      = parser.parse(value)

    query(schema, q, fields, limit, sort)(f)
  }

  /**
    * Returns all the entries.
    * @return all the entries.
    */
  def all(schema: Schema): Seq[Bit] = {
    Try { query(schema, new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None)(identity) } match {
      case Success(docs: Seq[Bit]) => docs
      case Failure(_)              => Seq.empty
    }
  }

  /**
    * Returns all entries applying the defined callback function
    *
    * @param f the callback function
    * @tparam B return type of f
    * @return all entries
    */
  def all[B](schema: Schema, f: Bit => B): Seq[B] = {
    Try { query(schema, new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None)(f) } match {
      case Success(docs: Seq[B]) => docs
      case Failure(_)            => Seq.empty
    }
  }

  /**
    * Executes a simple count [[Query]] using the given schema.
    * @return the query results as a list of entries.
    */
  def getCount: Int =
    executeCountQuery(this.getSearcher, new MatchAllDocsQuery(), Int.MaxValue) { doc =>
      doc.getField(_countField).numericValue().intValue()
    }.headOption.getOrElse(0)

  private def getFirstLastGroupBy(query: Query, schema: Schema, groupTagName: String, last: Boolean): Seq[Bit] = {
    val groupCollector = schema.fieldsMap(groupTagName).indexType match {
      case VARCHAR() => new TermGroupSelector(groupTagName)
      case _         => new TermGroupSelector(s"${groupTagName}_str")
    }

    val collector = AllGroupHeadsCollector.newCollector(groupCollector,
                                                        new Sort(new SortField("timestamp", SortField.Type.LONG, last)))
    val searcher = this.getSearcher
    searcher.search(query, collector)

    collector.retrieveGroupHeads().toSeq.map { id =>
      val doc = searcher.doc(id)
      Bit(
        timestamp = doc.getField("timestamp").numericValue().longValue(),
        value = NSDbNumericType(doc.getField("value").numericValue()),
        dimensions = Map.empty,
        tags = Map(doc.getField(groupTagName) match {
          case f if f.numericValue() != null => f.name() -> NSDbType(f.numericValue())
          case f                             => f.name() -> NSDbType(f.stringValue())
        })
      )
    }
  }

  /**
    * Group query results by groupTagName and return oldest value (min timestamp)
    * @param query query to be executed before grouping.
    * @param schema bit schema.
    * @param groupTagName tag used to group by.
    */
  def getFirstGroupBy(query: Query, schema: Schema, groupTagName: String): Seq[Bit] =
    getFirstLastGroupBy(query, schema, groupTagName, last = false)

  /**
    * Group query results by groupTagName and return most recent value (max timestamp)
    * @param query query to be executed before grouping.
    * @param schema bit schema.
    * @param groupTagName tag used to group by.
    */
  def getLastGroupBy(query: Query, schema: Schema, groupTagName: String): Seq[Bit] =
    getFirstLastGroupBy(query, schema, groupTagName, last = true)
}
