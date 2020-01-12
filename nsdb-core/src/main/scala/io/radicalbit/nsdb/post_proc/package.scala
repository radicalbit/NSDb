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

package io.radicalbit.nsdb
import io.radicalbit.nsdb.common.NSDbType
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, SelectSQLStatement, TemporalGroupByAggregation}
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementFailed

import scala.concurrent.{ExecutionContext, Future}

package object post_proc {

  /**
    * Applies, if needed, ordering and limiting to a sequence of chained partial results.
    * @param chainedResults sequence of chained partial results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @return the final result obtained from the manipulation of the partials.
    */
  def applyOrderingWithLimit(
      chainedResults: Future[Either[SelectStatementFailed, Seq[Bit]]],
      statement: SelectSQLStatement,
      schema: Schema)(implicit ec: ExecutionContext): Future[Either[SelectStatementFailed, Seq[Bit]]] = {
    chainedResults.map {
      case Right(seq) =>
        statement.groupBy match {
          case Some(TemporalGroupByAggregation(_, _, _)) =>
            val sortedResults = seq.sortBy(_.timestamp)
            statement.limit.map(_.value).map(v => Right(sortedResults.takeRight(v))) getOrElse Right(sortedResults)
          case _ =>
            val sortedResults = statement.order.map { order =>
              val o = schema.fieldsMap(order.dimension).indexType.ord
              implicit val ord: Ordering[Any] =
                if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse
                else o
              seq.sortBy(_.fields(statement.order.get.dimension)._1.rawValue)
            } getOrElse seq
            statement.limit.map(_.value).map(v => Right(sortedResults.take(v))) getOrElse Right(sortedResults)
        }
      case l @ Left(_) => l
    }
  }

  /**
    * This is a utility method to extract dimensions or tags from a Bit sequence in a functional way without having
    * the risk to throw dangerous exceptions.
    *
    * @param values the sequence of bits holding the fields to be extracted.
    * @param field the name of the field to be extracted.
    * @param extract the function defining how to extract the field from a given bit.
    * @return
    */
  def retrieveField(values: Seq[Bit], field: String, extract: Bit => Map[String, NSDbType]): Map[String, NSDbType] =
    values.headOption
      .flatMap(bit => extract(bit).get(field).map(x => Map(field -> x)))
      .getOrElse(Map.empty[String, NSDbType])

  /**
    * This is a utility method in charge to associate a dimension or a tag with the given count.
    * It extracts the field from a Bit sequence in a functional way without having the risk to throw dangerous exceptions.
    *
    * @param values the sequence of bits holding the field to be extracted.
    * @param count the value of the count to be associated with the field.
    * @param extract the function defining how to extract the field from a given bit.
    * @return
    */
  def retrieveCount(values: Seq[Bit], count: Int, extract: Bit => Map[String, NSDbType]): Map[String, NSDbType] =
    values.headOption
      .flatMap(bit => extract(bit).headOption.map(x => Map(x._1 -> NSDbType(count))))
      .getOrElse(Map.empty[String, NSDbType])

}
