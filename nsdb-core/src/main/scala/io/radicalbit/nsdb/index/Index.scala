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

import io.radicalbit.nsdb.index.lucene.AllGroupsAggregationCollector
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, SimpleMergedSegmentWarmer}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.apache.lucene.store.BaseDirectory
import org.apache.lucene.util.InfoStream

import scala.util.{Failure, Success, Try}

/**
  * Trait for a generic lucene index.
  * @tparam T the entity read and written in the index.
  */
trait Index[T] {

  /**
    * @return index base directory.
    */
  def directory: BaseDirectory

  /**
    * @return index entry identifier.
    */
  def _keyField: String

  /**
    * number of occurrences
    */
  val _countField: String = "_count"

  val _valueField = "value"

  private lazy val searcherManager: SearcherManager = new SearcherManager(directory, null)

  /**
    * @return a lucene [[IndexWriter]] to be used in write operations.
    */
  def getWriter =
    new IndexWriter(
      directory,
      new IndexWriterConfig(new StandardAnalyzer)
        .setUseCompoundFile(true)
        .setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(InfoStream.NO_OUTPUT))
    )

  /**
    * @return a lucene [[IndexSearcher]] to be used in search operations.
    */
  def getSearcher: IndexSearcher = searcherManager.acquire()

  /**
    * Refresh index content after a write operation.
    */
  def refresh(): Unit = searcherManager.maybeRefresh()

  /**
    * Validates a record before write it.
    * @param data the record to be validated.
    * @return a sequence of lucene [[Field]] that can be safely written in the index.
    */
  def validateRecord(data: T): Try[Seq[Field]]

  /**
    * Converts a lucene [[Document]] into an instance of T
    * @param document the lucene document to be converted.
    * @param fields fields that must be retrieved from the document.
    * @return the entry retrieved.
    */
  def toRecord(document: Document, fields: Seq[SimpleField]): T

  def write(fields: Set[Field])(implicit writer: IndexWriter): Try[Long] = {
    val doc = new Document
    fields.foreach(doc.add)
    Try(writer.addDocument(doc))
  }

  /**
    * Writes an entry into the index.
    * This method MUST NOT commit the writer.
    * @param data the entry to be written
    * @param writer a lucene [[IndexWriter]] to handle the write operation.
    * @return the lucene low level write operation return value.
    */
  protected def write(data: T)(implicit writer: IndexWriter): Try[Long]

  /**
    * Deletes an entry from the index.
    * @param data the entry to be deleted.
    * @param writer a lucene [[IndexWriter]] to handle the write operation.
    * @return the lucene low level delete operation return value.
    */
  def delete(data: T)(implicit writer: IndexWriter): Try[Long]

  /**
    * Deletes entries that fulfill the given query.
    * @param query the query to select the entries to be deleted.
    * @param writer a lucene [[IndexWriter]] to handle the write operation.
    * @return the lucene low level delete operation return value.
    */
  def delete(query: Query)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val result = writer.deleteDocuments(query)
      writer.forceMergeDeletes(true)
      result
    }
  }

  /**
    * Deletes all entries from the index.
    * @param writer a lucene [[IndexWriter]] to handle the write operation.
    * @return the lucene low level delete operation return value.
    */
  def deleteAll()(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val result = writer.deleteAll()
      writer.forceMergeDeletes(true)
      writer.flush()
      result
    }
  }

  private def executeQuery[B](searcher: IndexSearcher, query: Query, limit: Int, sort: Option[Sort])(
      f: Document => B): Seq[B] = {
    val hits =
      sort.fold(searcher.search(query, limit).scoreDocs)(sort => searcher.search(query, limit, sort).scoreDocs)
    (0 until hits.length).map { i =>
      val doc = searcher.doc(hits(i).doc)
      doc.add(new IntPoint(_countField, hits.length))
      f(doc)
    }
  }

  private def executeCountQuery[B](searcher: IndexSearcher, query: Query, limit: Int)(f: Document => B): Seq[B] = {
    val hits = searcher.search(query, limit).scoreDocs.length
    val d    = new Document()
    d.add(new LongPoint(_keyField, 0))
    d.add(new IntPoint(_valueField, hits))
    d.add(new IntPoint(_countField, hits))
    Seq(f(d))
  }

  private[index] def rawQuery(query: Query, limit: Int, sort: Option[Sort])(
      implicit searcher: IndexSearcher): Seq[Document] = {
    executeQuery(searcher, query, limit, sort)(identity)
  }

  private[index] def rawQuery[VT, S](query: Query,
                                     collector: AllGroupsAggregationCollector[VT, S],
                                     limit: Option[Int],
                                     sort: Option[Sort]): Seq[Document] = {
    this.getSearcher.search(query, collector)

    val sortedGroupMap = sort
      .flatMap(_.getSort.headOption)
      .map(s => collector.getOrderedMap(s))
      .getOrElse(collector.getGroupMap)
      .toSeq

    val limitedGroupMap = limit.map(sortedGroupMap.take).getOrElse(sortedGroupMap)

    limitedGroupMap.map {
      case (g, v) =>
        val doc = new Document
        doc.add(collector.indexField(g, collector.groupField))
        doc.add(collector.indexField(v, collector.aggField))
        doc.add(new LongPoint(_keyField, 0))
        doc
    }
  }

  /**
    * Executes a simple [[Query]].
    * @param query the [[Query]] to be executed.
    * @param fields sequence of fields that must be included in the result.
    * @param limit results limit.
    * @param sort optional lucene [[Sort]].
    * @param f function to obtain an element B from an element T.
    * @tparam B return type.
    * @return the query results as a list of entries.
    */
  def query[B](query: Query, fields: Seq[SimpleField], limit: Int, sort: Option[Sort])(f: T => B): Seq[B] = {
    if (fields.nonEmpty && fields.forall(_.count)) {
      executeCountQuery(this.getSearcher, query, limit) { doc =>
        f(toRecord(doc, fields))
      }
    } else
      executeQuery(this.getSearcher, query, limit, sort) { doc =>
        f(toRecord(doc, fields))
      }
  }

  /**
    * Executes an aggregated query.
    * @param query the [[Query]] to be executed.
    * @param collector the subclass of [[AllGroupsAggregationCollector]]
    * @param limit results limit.
    * @param sort optional lucene [[Sort]].
    * @return the query results as a list of entries.
    */
  def query(query: Query,
            collector: AllGroupsAggregationCollector[_, _],
            limit: Option[Int],
            sort: Option[Sort]): Seq[T] = {
    rawQuery(query, collector, limit, sort).map(d => toRecord(d, Seq.empty))
  }

  /**
    * Returns all the entries whiere `field` = `value`
    * @param field the field name to use to filter data.
    * @param value the value to check the field with.
    * @param fields sequence of fields that must be included in the result.
    * @param limit results limit.
    * @param sort optional lucene [[Sort]].
    * @param f function to obtain an element B from an element T.
    * @tparam B return type.
    * @return the manipulated Seq.
    */
  def query[B](field: String, value: String, fields: Seq[SimpleField], limit: Int, sort: Option[Sort] = None)(
      f: T => B): Seq[B] = {
    val parser = new QueryParser(field, new StandardAnalyzer())
    val q      = parser.parse(value)

    query(q, fields, limit, sort)(f)
  }

  /**
    * Returns all the entries.
    * @return all the entries.
    */
  def all: Seq[T] = {
    Try { query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None)(identity) } match {
      case Success(docs: Seq[T]) => docs
      case Failure(_)            => Seq.empty
    }
  }

  /**
    * Returns all entries applying the defined callback function
    *
    * @param f the callback function
    * @tparam B return type of f
    * @return all entries
    */
  def all[B](f: T => B): Seq[B] = {
    Try { query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None)(f) } match {
      case Success(docs: Seq[B]) => docs
      case Failure(_)            => Seq.empty
    }
  }

  def count(): Int = this.getSearcher.getIndexReader.numDocs()
}
