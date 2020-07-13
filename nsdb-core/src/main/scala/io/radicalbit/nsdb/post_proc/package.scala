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
import io.radicalbit.nsdb.statement.StatementParser._

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
      case Some(InternalCountStandardAggregation(_, _)) if statement.order.exists(_.dimension == "value") =>
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
    */
  def retrieveCount(values: Seq[Bit], count: Int, extract: Bit => Map[String, NSDbType]): Map[String, NSDbType] =
    values.headOption
      .flatMap(bit => extract(bit).headOption.map(x => Map(x._1 -> NSDbType(count))))
      .getOrElse(Map.empty[String, NSDbType])

  /**
    * This utility is used when, once aggregated, maps of the Bit should be keep equal
    * @param bits the sequence of bits holding the map to fold.
    * @param extract the function defining how to extract the field from a given bit.
    */
  def foldMapOfBit(bits: Seq[Bit], extract: Bit => Map[String, NSDbType]): Map[String, NSDbType] =
    bits.map(bit => extract(bit)).fold(Map.empty[String, NSDbType])(_ ++ _)

  def postProcessingTemporalQueryResult(
      schema: Schema,
      statement: SelectSQLStatement,
      aggr: InternalTemporalAggregation)(implicit ec: ExecutionContext): Seq[Bit] => Seq[Bit] = { res =>
    val v                              = schema.value.indexType.asInstanceOf[NumericType[_]]
    implicit val numeric: Numeric[Any] = v.numeric
    val chainedResult =
      res
        .groupBy(_.timestamp)
        .mapValues { bits =>
          val dimensions = foldMapOfBit(bits, bit => bit.dimensions)
          val tags       = foldMapOfBit(bits, bit => bit.tags)
          aggr match {
            case InternalCountTemporalAggregation =>
              Bit(bits.head.timestamp,
                  NSDbNumericType(bits.map(_.value.rawValue.asInstanceOf[Long]).sum),
                  dimensions,
                  tags)
            case InternalSumTemporalAggregation =>
              Bit(bits.head.timestamp, NSDbNumericType(bits.map(_.value.rawValue).sum), dimensions, tags)
            case InternalMaxTemporalAggregation =>
              Bit(bits.head.timestamp, NSDbNumericType(bits.map(_.value.rawValue).max), dimensions, tags)
            case InternalMinTemporalAggregation =>
              val nonZeroValues: Seq[Any] =
                bits.collect { case x if x.value.rawValue != numeric.zero => x.value.rawValue }
              Bit(bits.head.timestamp,
                  NSDbNumericType(if (nonZeroValues.isEmpty) numeric.zero else nonZeroValues.min),
                  dimensions,
                  tags)
            case InternalAvgTemporalAggregation =>
              // TODO: to implement
              throw new RuntimeException("Not implemented yet.")
          }
        }
        .values
        .toSeq

    applyOrderingWithLimit(chainedResult, statement, schema, Some(aggr))
  }

  def internalAggregationProcessing(bits: Seq[Bit],
                                    schema: Schema,
                                    aggregationType: InternalStandardAggregation): Bit = {
    val v                              = schema.value.indexType.asInstanceOf[NumericType[_]]
    implicit val numeric: Numeric[Any] = v.numeric
    aggregationType match {
      case _: InternalMaxStandardAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).max),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case _: InternalMinStandardAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).min),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case _: InternalSumStandardAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).sum),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case _: InternalFirstStandardAggregation => bits.minBy(_.timestamp)
      case _: InternalLastStandardAggregation  => bits.maxBy(_.timestamp)
    }
  }

}
