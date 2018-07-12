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

import io.radicalbit.nsdb.index.lucene.Index
import io.radicalbit.nsdb.statement.StatementParser.SimpleField
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._

import scala.util.{Failure, Success, Try}

/**
  * Trait for a generic lucene index.
  * @tparam T the entity read and written in the index.
  */
trait SimpleIndex[T] extends Index[T] {

  /**
    * Converts a lucene [[Document]] into an instance of T without checking a Schema.
    * @param document the lucene document to be converted.
    * @param fields fields that must be retrieved from the document.
    * @return the entry retrieved.
    */
  def toRecord(document: Document, fields: Seq[SimpleField]): T

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
}
