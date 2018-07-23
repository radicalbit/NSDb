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

package io.radicalbit.nsdb.index.lucene

import io.radicalbit.nsdb.common.protocol.Bit
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, IntPoint, LongPoint}
import org.apache.lucene.facet.range.{LongRange, LongRangeFacetCounts}
import org.apache.lucene.facet.{FacetResult, FacetsCollector}
import org.apache.lucene.index.{IndexNotFoundException, IndexWriter, IndexWriterConfig, SimpleMergedSegmentWarmer}
import org.apache.lucene.search._
import org.apache.lucene.store.Directory
import org.apache.lucene.util.InfoStream

import scala.util.{Success, Try}

trait Index[T] {

  /**
    * @return index base directory.
    */
  def directory: Directory

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
    *
    * @param data the record to be validated.
    * @return a sequence of lucene [[Field]] that can be safely written in the index.
    */
  def validateRecord(data: T): Try[Seq[Field]]

  /**
    * Writes an entry into the index.
    * This method MUST NOT commit the writer.
    *
    * @param data   the entry to be written
    * @param writer a lucene [[IndexWriter]] to handle the write operation.
    * @return the lucene low level write operation return value.
    */
  protected def write(data: T)(implicit writer: IndexWriter): Try[Long]

  /**
    * Deletes an entry from the index.
    *
    * @param data   the entry to be deleted.
    * @param writer a lucene [[IndexWriter]] to handle the write operation.
    * @return the lucene low level delete operation return value.
    */
  def delete(data: T)(implicit writer: IndexWriter): Try[Long]

  /**
    * Deletes entries that fulfill the given query.
    *
    * @param query  the query to select the entries to be deleted.
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
    *
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

  protected def executeQuery[B](searcher: IndexSearcher, query: Query, limit: Int, sort: Option[Sort])(
    f: Document => B): Seq[B] = {
    val hits =
      sort.fold(searcher.search(query, limit).scoreDocs)(sort => searcher.search(query, limit, sort).scoreDocs)
    (0 until hits.length).map { i =>
      val doc = searcher.doc(hits(i).doc)
      doc.add(new IntPoint(_countField, hits.length))
      f(doc)
    }
  }

  protected def executeCountQuery[B](searcher: IndexSearcher, query: Query, limit: Int)(f: Document => B): Seq[B] = {
    val hits = searcher.search(query, limit).scoreDocs.length
    val d = new Document()
    d.add(new LongPoint(_keyField, 0))
    d.add(new IntPoint(_valueField, hits))
    d.add(new IntPoint(_countField, hits))
    Seq(f(d))
  }

  def count(): Int = this.getSearcher.getIndexReader.numDocs()

  def close(): Unit = {
    directory.close()
  }

  def executeCountLongRangeFacet(
      searcher: IndexSearcher,
      query: Query,
      fieldName: String,
      ranges: Seq[LongRange]
  )(f: FacetResult => Seq[Bit]): Seq[Bit] = {
    val fc = new FacetsCollector
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: LongRangeFacetCounts = new LongRangeFacetCounts(fieldName, fc, ranges: _*)
    f(facets.getTopChildren(0, fieldName))
  }
}

object Index {
  def handleNoIndexResults[T](out: Try[Seq[T]]): Try[Seq[T]] = {
    out.recoverWith {
      case _: IndexNotFoundException => Success(Seq.empty)
    }
  }
}
