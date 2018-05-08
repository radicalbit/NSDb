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
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Generic Time series index based on entries of class [[Bit]].
  */
abstract class AbstractTimeSeriesIndex extends Index[Bit] with TypeSupport {

  override def _keyField: String = "timestamp"

  def write(data: Bit)(implicit writer: IndexWriter): Try[Long] = {
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

  override def toRecord(document: Document, fields: Seq[SimpleField]): Bit = {
    val dimensions: Map[String, JSerializable] =
      document.getFields.asScala
        .filterNot(f =>
          f.name() == _keyField || f.name() == _valueField || f.name() == _countField || (fields.nonEmpty &&
            !fields.exists(sf => sf.name == f.name() && !sf.count)))
        .map {
          case f if f.numericValue() != null => f.name() -> f.numericValue()
          case f                             => f.name() -> f.stringValue()
        }
        .toMap
    val aggregated: Map[String, JSerializable] =
      fields.filter(_.count).map(_.toString -> document.getField("_count").numericValue()).toMap

    val value = document.getField(_valueField).numericValue()
    Bit(timestamp = document.getField(_keyField).numericValue().longValue(),
        value = value,
        dimensions = dimensions ++ aggregated)
  }

  def delete(data: Bit)(implicit writer: IndexWriter): Try[Long] = {
    Try {
      val query  = LongPoint.newExactQuery(_keyField, data.timestamp)
      val result = writer.deleteDocuments(query)
      writer.forceMergeDeletes(true)
      result
    }
  }
}
