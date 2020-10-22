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

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.common.{NSDbLongType, NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.index.BIGINT
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
  final val `min(*)`   = "min(*)"
  final val `max(*)`   = "max(*)"

  final val lowerBoundField = "lowerBound"
  final val upperBoundField = "upperBound"

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
        val temporalSortedResults = statement.order match {
          case Some(DescOrderOperator("timestamp")) => chainedResults.sortBy(-_.timestamp)
          case _                                    => chainedResults.sortBy(_.timestamp)
        }
        statement.limit.map(_.value).map(v => temporalSortedResults.takeRight(v)) getOrElse temporalSortedResults
      case Some(InternalStandardAggregation(_, _: CountAggregation))
          if statement.order.exists(_.dimension == "value") =>
        implicit val ord: Ordering[Any] =
          (if (statement.order.get.isInstanceOf[DescOrderOperator]) Ordering[Long].reverse
           else Ordering[Long]).asInstanceOf[Ordering[Any]]
        val sortedResults = chainedResults.sortBy(_.value.rawValue)
        statement.limit.map(_.value).map(v => sortedResults.take(v)) getOrElse sortedResults
      case Some(InternalStandardAggregation(_, _: CountDistinctAggregation))
          if statement.order.exists(_.dimension == "value") =>
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
    * @param aggregationType aggregation type (temporal, count, sum etc.)
    * @return the final result obtained from the manipulation of the partials.
    */
  def limitAndOrder(chainedResults: ExecuteSelectStatementResponse,
                    aggregationType: Option[InternalAggregation] = None)(
      implicit ec: ExecutionContext): ExecuteSelectStatementResponse =
    chainedResults match {
      case SelectStatementExecuted(statement, values, schema) =>
        SelectStatementExecuted(statement, applyOrderingWithLimit(values, statement, schema, aggregationType), schema)
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
    val v                              = schema.value.indexType.asNumericType
    implicit val numeric: Numeric[Any] = v.numeric
    val chainedResult =
      res
        .groupBy(_.timestamp)
        .mapValues {
          case bits @ head +: _ =>
            val dimensions = foldMapOfBit(bits, bit => bit.dimensions)
            val tags       = foldMapOfBit(bits, bit => bit.tags)
            temporalAggregation.aggregation match {
              case _: CountAggregation =>
                Bit(head.timestamp, NSDbNumericType(bits.map(_.value.longValue).sum), dimensions, tags)
              case _: CountDistinctAggregation if finalStep =>
                Bit(head.timestamp, NSDbNumericType(bits.flatMap(_.uniqueValues).toSet.size.toLong), dimensions, tags)
              case _: CountDistinctAggregation =>
                Bit(head.timestamp, 0L, dimensions, tags, bits.flatMap(_.uniqueValues).toSet)
              case _: SumAggregation =>
                Bit(head.timestamp, NSDbNumericType(bits.map(_.value.rawValue).sum), dimensions, tags)
              case _: MaxAggregation =>
                Bit(head.timestamp, NSDbNumericType(bits.map(_.value.rawValue).max), dimensions, tags)
              case _: MinAggregation =>
                val nonZeroValues: Seq[Any] =
                  bits.collect { case x if x.value.rawValue != numeric.zero => x.value.rawValue }
                Bit(head.timestamp,
                    NSDbNumericType(if (nonZeroValues.isEmpty) numeric.zero else nonZeroValues.min),
                    dimensions,
                    tags)
              case _: AvgAggregation if finalStep =>
                val sum   = NSDbNumericType(bits.flatMap(_.tags.get("sum").map(_.rawValue)).sum)
                val count = NSDbNumericType(bits.flatMap(_.tags.get("count").map(_.rawValue)).sum)
                val avg   = if (count.rawValue == 0) NSDbNumericType(0.0) else NSDbNumericType(sum / count)
                Bit(
                  head.timestamp,
                  avg,
                  head.dimensions,
                  Map.empty[String, NSDbType]
                )
              case _: AvgAggregation =>
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

    if (finalStep)
      applyOrderingWithLimit(chainedResult, statement, schema, Some(temporalAggregation))
    else chainedResult
  }

  /**
    * Reduces temporal bucket given a schema and an aggregation.
    */
  def reduceSingleTemporalBucket(schema: Schema, temporalAggregation: InternalTemporalAggregation)(
      implicit mathContext: MathContext): Seq[Bit] => Option[Bit] = { res =>
    val v                              = schema.value.indexType.asNumericType
    implicit val numeric: Numeric[Any] = v.numeric

    res.sortBy(_.timestamp) match {
      case Nil => None
      case head :: Nil =>
        Some(
          Bit(head.timestamp,
              head.value,
              Map(upperBoundField -> head.timestamp, lowerBoundField -> head.timestamp),
              Map.empty)
        )
      case bits =>
        val head = bits.head
        val last = bits.last
        val dimensions =
          Map(upperBoundField -> NSDbNumericType(last.timestamp), lowerBoundField -> NSDbNumericType(head.timestamp))
        Some(
          temporalAggregation.aggregation match {
            case _: CountAggregation =>
              val count = NSDbLongType(bits.size)
              Bit(last.timestamp, count, dimensions, Map(`count(*)` -> count))
            case _: SumAggregation =>
              val sum = NSDbNumericType(bits.map(_.value.rawValue).sum)
              Bit(last.timestamp, sum, dimensions, Map(`sum(*)` -> sum))
            case _: MaxAggregation =>
              val max = NSDbNumericType(bits.map(_.value.rawValue).max)
              Bit(last.timestamp, max, dimensions, Map(`max(*)` -> max))
            case _: MinAggregation =>
              val min = NSDbNumericType(bits.map(_.value.rawValue).min)
              Bit(last.timestamp, min, dimensions, Map(`min(*)` -> min))
            case _: AvgAggregation =>
              val sum   = NSDbNumericType(bits.map(_.value.rawValue).sum)
              val count = NSDbNumericType(bits.size)
              val avg   = if (count.rawValue == 0) NSDbNumericType(0.0) else NSDbNumericType(sum / count)
              Bit(
                last.timestamp,
                avg,
                dimensions,
                Map(`avg(*)` -> avg)
              )
          }
        )
    }
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
    val v                              = schema.value.indexType.asNumericType
    implicit val numeric: Numeric[Any] = v.numeric
    standardAggregation.aggregation match {
      case _: CountAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.longValue).sum),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))

      case _: CountDistinctAggregation =>
        val uniqueValues = bits.foldLeft(Set.empty[NSDbType])((acc, b2) => acc ++ b2.uniqueValues)

        if (finalStep)
          Bit(0, uniqueValues.size.toLong, Map.empty, bits.headOption.map(_.tags).getOrElse(Map.empty))
        else
          Bit(0, 0L, Map.empty, bits.headOption.map(_.tags).getOrElse(Map.empty), uniqueValues)

      case _: MaxAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).max),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case _: MinAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).min),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case _: SumAggregation =>
        Bit(0,
            NSDbNumericType(bits.map(_.value.rawValue).sum),
            foldMapOfBit(bits, bit => bit.dimensions),
            foldMapOfBit(bits, bit => bit.tags))
      case _: FirstAggregation => bits.minBy(_.timestamp)
      case _: LastAggregation  => bits.maxBy(_.timestamp)
      case _: AvgAggregation if finalStep =>
        val sum   = NSDbNumericType(bits.flatMap(_.tags.get("sum").map(_.rawValue)).sum)
        val count = NSDbNumericType(bits.flatMap(_.tags.get("count").map(_.rawValue)).sum(BIGINT().numeric))
        val avg   = NSDbNumericType(sum / count)
        Bit(
          0L,
          avg,
          Map.empty[String, NSDbType],
          retrieveField(bits, standardAggregation.groupField, bit => bit.tags)
        )
      case _: AvgAggregation =>
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

    val v                              = schema.value.indexType.asNumericType
    implicit val numeric: Numeric[Any] = v.numeric

    val aggregationsReduced = aggregations.foldLeft(Map.empty[String, NSDbNumericType]) {
      case (acc, _: CountAggregation) =>
        val unlimitedCount =
          rawResults.map(_.tags.get(`count(*)`).flatMap(_.asNumericType).map(_.longValue).getOrElse(0L)).sum
        val limitedCount = statement.limit.map(limitOp => min(limitOp.value, unlimitedCount)).getOrElse(unlimitedCount)
        acc + (`count(*)` -> NSDbNumericType(limitedCount))

      case (acc, _: MaxAggregation) =>
        val localMax  = rawResults.flatMap(bit => bit.tags.get(`max(*)`).flatMap(_.asNumericType))
        val globalMax = localMax.reduceLeftOption((local1, local2) => if (local1 >= local2) local1 else local2)
        globalMax.fold(acc)(globalMax => acc + (`max(*)` -> globalMax))

      case (acc, _: MinAggregation) =>
        val localMins = rawResults.flatMap(bit => bit.tags.get(`min(*)`).flatMap(_.asNumericType))
        val globalMin = localMins.reduceLeftOption((local1, local2) => if (local1 <= local2) local1 else local2)
        globalMin.fold(acc)(globalMin => acc + (`min(*)` -> globalMin))

      case (acc, _: SumAggregation) =>
        val sum = NSDbNumericType(rawResults.flatMap(_.tags.get(`sum(*)`).map(_.rawValue)).sum)
        acc + (`sum(*)` -> sum)

      case (acc, _: AvgAggregation) =>
        val sum   = NSDbNumericType(rawResults.flatMap(_.tags.get(`sum(*)`).map(_.rawValue)).sum)
        val count = NSDbNumericType(rawResults.flatMap(_.tags.get(`count(*)`).map(_.rawValue)).sum(BIGINT().numeric))
        if (finalStep) {
          val avg = if (count.rawValue == numeric.zero) NSDbNumericType(numeric.zero) else NSDbNumericType(sum / count)
          acc + (`avg(*)` -> avg)
        } else {
          acc + (`sum(*)` -> NSDbNumericType(sum)) + (`count(*)` -> NSDbNumericType(count))
        }

      case (acc, _) => acc
    }

    val uniqueValues = rawResults.foldLeft(Set.empty[NSDbType])((acc, b2) => acc ++ b2.uniqueValues)

    //only one count distinct is allowed. This has been checked previously in the flow
    val allAggregationReduced = aggregations.find(_.isInstanceOf[CountDistinctAggregation]) match {
      case Some(aggregation) if finalStep =>
        aggregationsReduced + (s"count(distinct ${aggregation.fieldName})" -> NSDbLongType(uniqueValues.size))
      case None => aggregationsReduced
    }

    val finalUniqueValues = if (finalStep) Set.empty[NSDbType] else uniqueValues

    if (fields.nonEmpty) {
      applyOrderingWithLimit(
        rawResults.map { bit =>
          bit.copy(tags = bit.tags - `sum(*)` - `count(*)` ++ allAggregationReduced, uniqueValues = finalUniqueValues)
        },
        statement,
        schema
      )
    } else {
      Seq(Bit(0, NSDbNumericType(numeric.zero), Map.empty, allAggregationReduced, finalUniqueValues))
    }
  }

}
