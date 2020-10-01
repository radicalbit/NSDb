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

import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.index.lucene.Index
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.statement.FieldsParser.SimpleField
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search._
import org.apache.lucene.search.grouping._
import org.apache.lucene.util.BytesRef

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

  private def toRecord(schema: Schema, document: Document, fields: Seq[SimpleField]): Bit = {

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
                sf.name == f.name() || sf.name.trim == "*"
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

    val value = document.getField(_valueField).numericValue()
    Bit(
      timestamp = document.getField(_keyField).numericValue().longValue(),
      value = NSDbNumericType(value),
      dimensions = dimensions,
      tags = tags
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
    * @return the query results as a list of entries.
    */
  def query(schema: Schema, query: Query, fields: Seq[SimpleField], limit: Int, sort: Option[Sort]): Seq[Bit] = {
    executeQuery(this.getSearcher, query, limit, sort) { doc =>
      toRecord(schema: Schema, doc, fields)
    }
  }

  /**
    * Returns all the entries.
    * @return all the entries.
    */
  def all(schema: Schema): Seq[Bit] = {
    Try { query(schema, new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None) } match {
      case Success(docs: Seq[Bit]) => docs
      case Failure(_)              => Seq.empty
    }
  }

  /**
    * Executes a simple count [[Query]] using the given schema.
    * @return the query results as a list of entries.
    */
  def getCount(query: Query = new MatchAllDocsQuery(), limit: Int = Int.MaxValue): Long =
    executeCountQuery(this.getSearcher, query, limit) { doc =>
      doc.getField(_countField).numericValue().intValue()
    }

  private def headItemOfGroup(query: Query,
                              schema: Schema,
                              groupTagName: String,
                              last: Boolean,
                              sortField: String): Seq[Bit] = {

    val groupSelector = schema.fieldsMap(groupTagName).indexType match {
      case VARCHAR() => new TermGroupSelector(groupTagName)
      case _         => new TermGroupSelector(s"${groupTagName}_str")
    }

    val sortType = schema.fieldsMap.get(sortField).map(_.indexType.sortType).getOrElse(SortField.Type.DOC)

    val collector =
      AllGroupHeadsCollector.newCollector(groupSelector, new Sort(new SortField(sortField, sortType, last)))

    val searcher = this.getSearcher
    searcher.search(query, collector)

    collector.retrieveGroupHeads().map(searcher.doc).collect {
      case doc if doc.getField(groupTagName) != null =>
        Bit(
          timestamp = doc.getField(_keyField).numericValue().longValue(),
          value = NSDbNumericType(doc.getField(_valueField).numericValue()),
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
    headItemOfGroup(query, schema, groupTagName, last = false, _keyField)

  /**
    * Group query results by groupTagName and return most recent value (max timestamp)
    * @param query query to be executed before grouping.
    * @param schema bit schema.
    * @param groupTagName tag used to group by.
    */
  def getLastGroupBy(query: Query, schema: Schema, groupTagName: String): Seq[Bit] =
    headItemOfGroup(query, schema, groupTagName, last = true, _keyField)

  /**
    * Group query results by groupTagName and return max value
    * @param query query to be executed before grouping.
    * @param schema bit schema.
    * @param groupTagName tag used to group by.
    */
  def getMaxGroupBy(query: Query, schema: Schema, groupTagName: String): Seq[Bit] =
    headItemOfGroup(query, schema, groupTagName, last = true, _valueField)

  /**
    * Group query results by groupTagName and return min value
    * @param query query to be executed before grouping.
    * @param schema bit schema.
    * @param groupTagName tag used to group by.
    */
  def getMinGroupBy(query: Query, schema: Schema, groupTagName: String): Seq[Bit] =
    headItemOfGroup(query, schema, groupTagName, last = false, _valueField)

  def countDistinct(query: Query, schema: Schema, groupTagName: String): Seq[Bit] = {
    val groupSelector = schema.fieldsMap(groupTagName).indexType match {
      case VARCHAR() => new TermGroupSelector(groupTagName)
      case _         => new TermGroupSelector(s"${groupTagName}_str")
    }

    val groupSort = new Sort()

    val searcher = this.getSearcher

    val firstPassGroupingCollector =
      new FirstPassGroupingCollector[BytesRef](groupSelector, groupSort, 1000)

    searcher.search(query, firstPassGroupingCollector)

    val gs = new TermGroupSelector("value_str");

    val groupsOpt = Option(firstPassGroupingCollector.getTopGroups(0))

    groupsOpt match {
      case Some(inputGroups) =>
        val distinctValuesCollector =
          new DistinctValuesCollector[BytesRef, BytesRef](firstPassGroupingCollector.getGroupSelector, inputGroups, gs)
        searcher.search(query, distinctValuesCollector)

        val groups = distinctValuesCollector.getGroups.asScala

        groups.map(
          g =>
            Bit(
              0,
              0,
              Map.empty,
              Map(groupTagName -> schema.tags(groupTagName).indexType.deserialize(g.groupValue.bytes)),
              g.uniqueValues.asScala.map { v =>
                schema.value.indexType.deserialize(new String(v.bytes).stripSuffix("_str").getBytes)
              }.toSet,
          )
        )

      case None =>
        Seq.empty
    }
  }
}
