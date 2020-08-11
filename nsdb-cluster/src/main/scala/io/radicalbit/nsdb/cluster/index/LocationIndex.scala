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

package io.radicalbit.nsdb.cluster.index

import io.radicalbit.nsdb.index.SimpleIndex
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.statement.FieldsParser.SimpleField
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, Term}
import org.apache.lucene.search._
import org.apache.lucene.store.Directory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Index for storing metric locations.
  * @param directory index bae directory.
  */
class LocationIndex(override val directory: Directory) extends SimpleIndex[Location] {
  override val _keyField: String = "_metric"

  override def validateRecord(data: Location): Try[Seq[Field]] = {
    Success(
      Seq(
        new StringField(_keyField, data.metric, Store.YES),
        new StringField("node", data.node, Store.YES),
        new LongPoint("from", data.from),
        new LongPoint("to", data.to),
        new NumericDocValuesField("from", data.from),
        new NumericDocValuesField("to", data.to),
        new StoredField("from", data.from),
        new StoredField("to", data.to)
      )
    )
  }

  override def write(data: Location)(implicit writer: IndexWriter): Try[Long] = {
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

  override def toRecord(document: Document, fields: Seq[SimpleField]): Location = {
    val fields = document.getFields.asScala.map(f => f.name() -> f).toMap
    Location(
      document.get(_keyField),
      document.get("node"),
      fields("from").numericValue().longValue(),
      fields("to").numericValue().longValue()
    )
  }

  def getLocationsForMetric(metric: String): Seq[Location] = {
    val queryTerm = new TermQuery(new Term(_keyField, metric))

    Try(query(queryTerm, Seq.empty, Integer.MAX_VALUE, None)(identity)) match {
      case Success(metadataSeq) => metadataSeq
      case Failure(_)           => Seq.empty
    }
  }

  def getLocationForMetricAtTime(metric: String, t: Long): Option[Location] = {
    val builder = new BooleanQuery.Builder()
    builder.add(LongPoint.newRangeQuery("to", t, Long.MaxValue), BooleanClause.Occur.SHOULD)
    builder.add(LongPoint.newRangeQuery("from", 0, t), BooleanClause.Occur.SHOULD).build()

    Try(query(builder.build(), Seq.empty, Integer.MAX_VALUE, None)(identity).headOption) match {
      case Success(metadataSeq) => metadataSeq
      case Failure(_)           => None
    }
  }

  def deleteByMetric(metric: String)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val result = writer.deleteDocuments(new TermQuery(new Term(_keyField, metric)))
      writer.forceMergeDeletes(true)
      result
    }
  }

  override def delete(data: Location)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val builder = new BooleanQuery.Builder()
      builder.add(new TermQuery(new Term(_keyField, data.metric)), BooleanClause.Occur.MUST)
      builder.add(new TermQuery(new Term("node", data.node)), BooleanClause.Occur.MUST)
      builder.add(LongPoint.newExactQuery("from", data.from), BooleanClause.Occur.MUST)
      builder.add(LongPoint.newExactQuery("to", data.to), BooleanClause.Occur.MUST)

      val query = builder.build()

      val result = writer.deleteDocuments(query)
      writer.forceMergeDeletes(true)
      result
    }

  }
}
