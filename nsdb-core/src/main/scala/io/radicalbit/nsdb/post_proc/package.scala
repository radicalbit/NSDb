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
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, SelectSQLStatement}
import io.radicalbit.nsdb.index.NumericType
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  ExecuteSelectStatementResponse,
  SelectStatementExecuted,
  SelectStatementFailed
}
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser.{
  InternalAggregation,
  InternalCountSimpleAggregation,
  InternalTemporalAggregation
}

import scala.concurrent.ExecutionContext

package object post_proc {

  /**
    * Applies, if needed, ordering and limiting to a sequence of chained partial results.
    * @param chainedResults sequence of chained partial results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @param aggregationType aggregation type (temporal, count, sum etc.)
    * @return the final result obtained from the manipulation of the partials.
    */
  def applyOrderingWithLimit(
      chainedResults: Seq[Bit],
      statement: SelectSQLStatement,
      schema: Schema,
      aggregationType: Option[InternalAggregation] = None)(implicit ec: ExecutionContext): Seq[Bit] = {
    val sortedResults = aggregationType match {
      case Some(_: InternalTemporalAggregation) =>
        val temporalSortedResults = chainedResults.sortBy(_.timestamp)
        statement.limit.map(_.value).map(v => temporalSortedResults.takeRight(v)) getOrElse temporalSortedResults
      case Some(InternalCountSimpleAggregation(_, _)) if statement.order.exists(_.dimension == "value") =>
        implicit val ord: Ordering[Any] =
          (if (statement.order.get.isInstanceOf[DescOrderOperator]) Ordering[Long].reverse
           else Ordering[Long]).asInstanceOf[Ordering[Any]]
        val sortedResults = chainedResults.sortBy(_.value.rawValue)
        statement.limit.map(_.value).map(v => sortedResults.take(v)) getOrElse sortedResults
      case _ =>
        val sortedResults = statement.order.map { order =>
          val o = schema.fieldsMap(order.dimension).indexType.ord
          implicit val ord: Ordering[Any] =
            if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse
            else o
          chainedResults.sortBy(_.fields(statement.order.get.dimension)._1.rawValue)
        } getOrElse chainedResults
        statement.limit.map(_.value).map(v => sortedResults.take(v)) getOrElse sortedResults
    }
    statement.limit
      .map(_.value)
      .map(v => sortedResults.take(v)) getOrElse sortedResults
  }

  /**
    * Applies, if needed, ordering and limiting to a sequence of chained partial results.
    * @param chainedResults sequence of chained partial results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @param aggregationType aggregation type (temporal, count, sum etc.)
    * @return the final result obtained from the manipulation of the partials.
    */
  def limitAndOrder(chainedResults: ExecuteSelectStatementResponse,
                    statement: SelectSQLStatement,
                    schema: Schema,
                    aggregationType: Option[InternalAggregation] = None)(
      implicit ec: ExecutionContext): ExecuteSelectStatementResponse =
    chainedResults match {
      case SelectStatementExecuted(statement, values) =>
        SelectStatementExecuted(statement, applyOrderingWithLimit(values, statement, schema, aggregationType))
      case e: SelectStatementFailed => e
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

  def postProcessingTemporalQueryResult(
      schema: Schema,
      statement: SelectSQLStatement,
      aggr: InternalTemporalAggregation)(implicit ec: ExecutionContext): Seq[Bit] => Seq[Bit] = { res =>
    val v                              = schema.value.indexType.asInstanceOf[NumericType[_]]
    implicit val numeric: Numeric[Any] = v.numeric
    applyOrderingWithLimit(
      res
        .groupBy(_.timestamp)
        .mapValues(
          v =>
            aggr match {
              case StatementParser.InternalCountTemporalAggregation =>
                Bit(v.head.timestamp,
                    NSDbNumericType(v.map(_.value.rawValue.asInstanceOf[Long]).sum),
                    v.head.dimensions,
                    v.head.tags)
              case StatementParser.InternalSumTemporalAggregation =>
                Bit(v.head.timestamp, NSDbNumericType(v.map(_.value.rawValue).sum), v.head.dimensions, v.head.tags)
              case StatementParser.InternalMaxTemporalAggregation =>
                Bit(v.head.timestamp, NSDbNumericType(v.map(_.value.rawValue).max), v.head.dimensions, v.head.tags)
              case StatementParser.InternalMinTemporalAggregation =>
                val nonZeroValues: Seq[Any] =
                  v.collect { case x if x.value.rawValue != numeric.zero => x.value.rawValue }
                Bit(v.head.timestamp,
                    NSDbNumericType(if (nonZeroValues.isEmpty) numeric.zero else nonZeroValues.min),
                    v.head.dimensions,
                    v.head.tags)
          }
        )
        .values
        .toSeq,
      statement,
      schema,
      Some(aggr)
    )
  }

}
