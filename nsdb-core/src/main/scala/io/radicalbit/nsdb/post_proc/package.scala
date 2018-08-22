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

package io.radicalbit.nsdb
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, SelectSQLStatement}
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementFailed

import scala.concurrent.{ExecutionContext, Future}

package object post_proc {

  /**
    * Applies, if needed, ordering and limiting to a sequence of chained partial results.
    * @param chainedResults sequence of chained results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @return a single result obtained from the manipulation of the partials.
    */
  def applyOrderingWithLimit(
      chainedResults: Future[Either[SelectStatementFailed, Seq[Bit]]],
      statement: SelectSQLStatement,
      schema: Schema)(implicit ec: ExecutionContext): Future[Either[SelectStatementFailed, Seq[Bit]]] = {
    chainedResults.map(s =>
      s.map { seq =>
        val maybeSorted = if (statement.order.isDefined) {
          val o = schema.fields.find(_.name == statement.order.get.dimension).get.indexType.ord
          implicit val ord: Ordering[JSerializable] =
            if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse
            else o
          seq.sortBy(_.fields(statement.order.get.dimension)._1)
        } else seq
        if (statement.limit.isDefined) maybeSorted.take(statement.limit.get.value) else maybeSorted
    })
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
  def retrieveField(values: Seq[Bit],
                    field: String,
                    extract: Bit => Map[String, JSerializable]): Map[String, JSerializable] =
    values.headOption
      .flatMap(bit => extract(bit).get(field).map(x => Map(field -> x)))
      .getOrElse(Map.empty[String, JSerializable])

  /**
    * This is a utility method in charge to associate a dimension or a tag with the given count.
    * It extracts the field from a Bit sequence in a functional way without having the risk to throw dangerous exceptions.
    *
    * @param values the sequence of bits holding the field to be extracted.
    * @param count the value of the count to be associated with the field.
    * @param extract the function defining how to extract the field from a given bit.
    * @return
    */
  def retrieveCount(values: Seq[Bit],
                    count: Int,
                    extract: Bit => Map[String, JSerializable]): Map[String, JSerializable] =
    values.headOption
      .flatMap(bit => extract(bit).headOption.map(x => Map(x._1 -> count.asInstanceOf[JSerializable])))
      .getOrElse(Map.empty[String, JSerializable])

}
