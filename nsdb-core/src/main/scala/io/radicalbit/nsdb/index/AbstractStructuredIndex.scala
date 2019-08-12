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

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType, FieldClassType, TagFieldType}
import io.radicalbit.nsdb.index.lucene.Index
import io.radicalbit.nsdb.model.{Schema, TimeRange}
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import io.radicalbit.nsdb.statement.{
  InternalCountTemporalAggregation,
  InternalSumTemporalAggregation,
  InternalTemporalAggregationType
}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.facet.range.{LongRange, LongRangeFacetCounts, LongRangeFacetLongSum}
import org.apache.lucene.facet.{FacetResult, Facets, FacetsCollector}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, Query, Sort}

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

  override def validateRecord(bit: Bit): Try[Seq[Field]] =
    validateSchemaTypeSupport(bit)
      .map(se => se.flatMap(elem => elem.indexType.indexField(elem.name, elem.value)))

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
          schema.fields
            .find(fieldSchema => fieldSchema.name == f.name && fieldSchema.fieldClassType == fieldClassType)
            .forall { _ =>
              f.name() == _keyField || f.name() == _valueField || f.name() == _countField || (fields.nonEmpty &&
              !fields.exists { sf =>
                (sf.name == f.name() || sf.name.trim == "*") && !sf.count
              })
            }
        }
        .map {
          case f if f.numericValue() != null => f.name() -> f.numericValue()
          case f                             => f.name() -> f.stringValue()
        }
    }

    val dimensions: Map[String, JSerializable] = extractFields(schema, document, fields, DimensionFieldType).toMap
    val tags: Map[String, JSerializable]       = extractFields(schema, document, fields, TagFieldType).toMap

    val aggregated: Map[String, JSerializable] =
      fields.filter(_.count).map(_.toString -> document.getField("_count").numericValue()).toMap

    val value = document.getField(_valueField).numericValue()
    Bit(
      timestamp = document.getField(_keyField).numericValue().longValue(),
      value = value,
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

  def executeLongRangeFacet(
      searcher: IndexSearcher,
      query: Query,
      aggregationType: InternalTemporalAggregationType,
      rangeFieldName: String,
      valueFieldName: String,
      ranges: Seq[TimeRange]
  )(f: FacetResult => Seq[Bit]): Seq[Bit] = {
    val luceneRanges = ranges.map(r =>
      new LongRange(s"${r.lowerBound}-${r.upperBound}", r.lowerBound, r.lowerInclusive, r.upperBound, r.upperInclusive))
    val fc = new FacetsCollector
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: Facets = aggregationType match {
      case InternalCountTemporalAggregation => new LongRangeFacetCounts(rangeFieldName, fc, luceneRanges: _*)
      case InternalSumTemporalAggregation =>
        new LongRangeFacetLongSum(rangeFieldName, valueFieldName, fc, luceneRanges: _*)
    }
    f(facets.getTopChildren(0, rangeFieldName))
  }

}
