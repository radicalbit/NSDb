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

package io.radicalbit.nsdb.cluster.index

import io.radicalbit.nsdb.index.SimpleIndex
import io.radicalbit.nsdb.index.lucene.Index._
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, Term}
import org.apache.lucene.search._
import org.apache.lucene.store.Directory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Metric Info.
  * @param metric the metric.
  * @param shardInterval shard interval for the metric in milliseconds
  */
case class MetricInfo(metric: String, shardInterval: Long)

/**
  * Index for storing metric info (shard interval).
  * @param directory index bae directory.
  */
class MetricInfoIndex(override val directory: Directory) extends SimpleIndex[MetricInfo] {
  override val _keyField: String = "_metric"

  override def validateRecord(data: MetricInfo): Try[Seq[Field]] = {
    Success(
      Seq(
        new StringField(_keyField, data.metric, Store.YES),
        new StoredField("shardInterval", data.shardInterval)
      )
    )
  }

  override def write(data: MetricInfo)(implicit writer: IndexWriter): Try[Long] = {
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

  override def toRecord(document: Document, fields: Seq[SimpleField]): MetricInfo = {
    val fields = document.getFields.asScala.map(f => f.name() -> f).toMap
    MetricInfo(
      document.get(_keyField),
      fields("shardInterval").numericValue().longValue()
    )
  }

  def getMetricInfo(metric: String): Option[MetricInfo] = {
    val results = handleNoIndexResults(Try {
      val queryTerm = new TermQuery(new Term(_keyField, metric))

      implicit val searcher: IndexSearcher = getSearcher

      query(queryTerm, Seq.empty, Integer.MAX_VALUE, None)(identity)
    })
    results match {
      case Success(metricInfoes) => metricInfoes.headOption
      case Failure(_)            => None
    }
  }

  override def delete(data: MetricInfo)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val queryTerm = new TermQuery(new Term(_keyField, data.metric))

      val result = writer.deleteDocuments(queryTerm)
      writer.forceMergeDeletes(true)
      result
    }
  }

  def deleteByMetric(metric: String)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val queryTerm = new TermQuery(new Term(_keyField, metric))

      val result = writer.deleteDocuments(queryTerm)
      writer.forceMergeDeletes(true)
      result
    }
  }
}
