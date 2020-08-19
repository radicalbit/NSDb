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
import java.math.MathContext

import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{
  Aggregation,
  AvgAggregation,
  CountAggregation,
  DescOrderOperator,
  FirstAggregation,
  LastAggregation,
  MaxAggregation,
  MinAggregation,
  SelectSQLStatement,
  SumAggregation
}
import io.radicalbit.nsdb.index.{BIGINT, NumericType}
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  ExecuteSelectStatementResponse,
  SelectStatementExecuted,
  SelectStatementFailed
}
import io.radicalbit.nsdb.statement.FieldsParser.SimpleField
import io.radicalbit.nsdb.statement.StatementParser._

import scala.concurrent.ExecutionContext
import scala.math.min

package object post_proc {

  final val `count(*)` = "count(*)"
  final val `sum(*)`   = "sum(*)"
  final val `avg(*)`   = "avg(*)"

  /**
    * Applies, if needed, ordering and limiting to a sequence of chained partial results.
    * @param chainedResults sequence of chained partial results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @param aggregationType aggregation type (temporal, count, sum etc.)
    * @return the final result obtained from the manipulation of the partials.
    */
  def applyOrderingWithLimit(chainedResults: Seq[Bit],
                             statement: SelectSQLStatement,
                             schema: Schema,
                             aggregationType: Option[InternalAggregation] = None): Seq[Bit] = {
    val sortedResults = aggregationType match {
      case Some(_: InternalTemporalAggregation) =>
        val temporalSortedResults = chainedResults.sortBy(_.timestamp)
        statement.limit.map(_.value).map(v => temporalSortedResults.takeRight(v)) getOrElse temporalSortedResults
      case Some(InternalStandardAggregation(_, CountAggregation)) if statement.order.exists(_.dimension == "value") =>
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
      temporalAggregation: InternalTemporalAggregation,
      finalStep: Boolean = true)(implicit mathContext: MathContext): Seq[Bit] => Seq[Bit] = { res =>
    val v                              = schema.value.indexType.asInstanceOf[NumericType[_]]
    implicit val numeric: Numeric[Any] = v.numeric
    val chainedResult =
      res
        .groupBy(_.timestamp)
        .mapValues {
          case bits @ head +: _ =>
            val dimensions = foldMapOfBit(bits, bit => bit.dimensions)
            val tags       = foldMapOfBit(bits, bit => bit.tags)
            temporalAggregation.aggregation match {
              case CountAggregation =>
                Bit(head.timestamp,
                    NSDbNumericType(bits.map(_.value.rawValue.asInstanceOf[Long]).sum),
                    dimensions,
                    tags)
              case SumAggregation =>
                Bit(head.timestamp, NSDbNumericType(bits.map(_.value.rawValue).sum), dimensions, tags)
              case MaxAggregation =>
                Bit(head.timestamp, NSDbNumericType(bits.map(_.value.rawValue).max), dimensions, tags)
              case MinAggregation =>
                val nonZeroValues: Seq[Any] =
                  bits.collect { case x if x.value.rawValue != numeric.zero => x.value.rawValue }
                Bit(head.timestamp,
                    NSDbNumericType(if (nonZeroValues.isEmpty) numeric.zero else nonZeroValues.min),
                    dimensions,
                    tags)
              case AvgAggregation if finalStep =>
                val sum   = NSDbNumericType(bits.flatMap(_.tags.get("sum").map(_.rawValue)).sum)
                val count = NSDbNumericType(bits.flatMap(_.tags.get("count").map(_.rawValue)).sum)
                val avg   = if (count.rawValue == 0) NSDbNumericType(0.0) else NSDbNumericType(sum / count)
                Bit(
                  head.timestamp,
                  avg,
                  head.dimensions,
                  Map.empty[String, NSDbType]
                )
              case AvgAggregation =>
                Bit(
                  head.timestamp,
                  0.0,
                  head.dimensions,
                  Map(
                    "sum"   -> NSDbNumericType(bits.flatMap(_.tags.get("sum").map(_.rawValue)).sum),
                    "count" -> NSDbNumericType(bits.flatMap(_.tags.get("count").map(_.rawValue)).sum)
                  )
                )

            }
        }
        .values
        .toSeq

    applyOrderingWithLimit(chainedResult, statement, schema, Some(temporalAggregation))
  }

  /**
    * Reduces a sequence of partial results given an aggregation.
    * The reduce operation can be final or partial.
    * @param bits the partial results.
    * @param schema metric schema.
    * @param standardAggregation the aggregation provided inside the query.
    * @param finalStep whether it's the final reduce to be calculated or not.
    * @return the reduces results.
    */
  def internalAggregationReduce(bits: Seq[Bit],
                                schema: Schema,
                                standardAggregation: InternalStandardAggregation,
                                finalStep: Boolean = true)(implicit mathContext: MathContext): Bit = {
    val v                              = schema.value.indexType.asInstanceOf[NumericType[_]]
    implicit val numeric: Numeric[Any] = v.numeric
    standardAggregation.aggregation match {
      case CountAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue.asInstanceOf[Long]).sum),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case MaxAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).max),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case MinAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).min),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case SumAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).sum),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case FirstAggregation => bits.minBy(_.timestamp)
      case LastAggregation  => bits.maxBy(_.timestamp)
      case AvgAggregation if finalStep =>
        val sum   = NSDbNumericType(bits.flatMap(_.tags.get("sum").map(_.rawValue)).sum)
        val count = NSDbNumericType(bits.flatMap(_.tags.get("count").map(_.rawValue)).sum(BIGINT().numeric))
        val avg   = NSDbNumericType(sum / count)
        Bit(
          0L,
          avg,
          Map.empty[String, NSDbType],
          retrieveField(bits, standardAggregation.groupField, bit => bit.tags)
        )
      case AvgAggregation =>
        Bit(
          0L,
          NSDbNumericType(0),
          Map.empty[String, NSDbType],
          Map(
            standardAggregation.groupField -> bits.flatMap(_.tags.get(standardAggregation.groupField)).head,
            "sum"                          -> NSDbNumericType(bits.flatMap(_.tags.get("sum").map(_.rawValue)).sum),
            "count"                        -> NSDbNumericType(bits.flatMap(_.tags.get("count").map(_.rawValue)).sum(BIGINT().numeric))
          )
        )
    }
  }

  /**
    * Perform all reduction operations for Global Aggregated queries.
    * @param rawResults sequence of partial results.
    * @param fields the fields to include in the final results.
    * @param statement the original sql statement.
    * @param schema metric schema.
    * @param finalStep whether it's the final reduce to be calculated or not.
    * @return the reduced results.
    */
  def globalAggregationReduce(rawResults: Seq[Bit],
                              fields: List[SimpleField],
                              aggregations: List[Aggregation],
                              statement: SelectSQLStatement,
                              schema: Schema,
                              finalStep: Boolean = true)(implicit mathContext: MathContext): Seq[Bit] = {

    val v                              = schema.value.indexType.asInstanceOf[NumericType[_]]
    implicit val numeric: Numeric[Any] = v.numeric

    val aggregationsReduced = aggregations.map {
      case CountAggregation =>
        val unlimitedCount = rawResults.map(_.tags(`count(*)`).rawValue.asInstanceOf[Long]).sum
        val limitedCount   = statement.limit.map(limitOp => min(limitOp.value, unlimitedCount)).getOrElse(unlimitedCount)
        `count(*)` -> NSDbNumericType(limitedCount)
      case AvgAggregation =>
        val sum   = NSDbNumericType(rawResults.flatMap(_.tags.get(`sum(*)`).map(_.rawValue)).sum)
        val count = NSDbNumericType(rawResults.flatMap(_.tags.get(`count(*)`).map(_.rawValue)).sum(BIGINT().numeric))
        if (finalStep) {
          val avg = if (count.rawValue == numeric.zero) NSDbNumericType(numeric.zero) else NSDbNumericType(sum / count)
          `avg(*)` -> avg
        } else {
          `sum(*)`   -> NSDbNumericType(sum)
          `count(*)` -> NSDbNumericType(count)
        }
    }.toMap

    if (fields.nonEmpty) {
      applyOrderingWithLimit(
        rawResults.map { bit =>
          bit.copy(tags = bit.tags - `sum(*)` - `count(*)` ++ aggregationsReduced)
        },
        statement,
        schema
      )
    } else {
      Seq(Bit(0, NSDbNumericType(numeric.zero), Map.empty, aggregationsReduced))
    }
  }

}
