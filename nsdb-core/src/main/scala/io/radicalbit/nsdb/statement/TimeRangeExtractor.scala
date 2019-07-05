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

package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.statement.StatementParser.NBuckets
import org.apache.lucene.facet.range.LongRange
import spire.implicits._
import spire.math.Interval
import spire.math.interval.{Closed, Open, Unbound}

import scala.annotation.tailrec

/**
  * Provides a utility method to retrieve a list of [[Interval]] from a given [[Expression]]. That interval list will be used to identify shards to execute query against.
  */
object TimeRangeExtractor {

  def extractTimeRange(exp: Option[Expression]): List[Interval[Long]] = {
    exp match {
      case Some(EqualityExpression("timestamp", value: Long)) => List(Interval.point(value))
      case Some(ComparisonExpression("timestamp", operator: ComparisonOperator, value: Long)) =>
        operator match {
          case GreaterThanOperator      => List(Interval.fromBounds(Open(value), Unbound()))
          case GreaterOrEqualToOperator => List(Interval.fromBounds(Closed(value), Unbound()))
          case LessThanOperator         => List(Interval.openUpper(0, value))
          case LessOrEqualToOperator    => List(Interval.closed(0, value))
        }
      case Some(RangeExpression("timestamp", v1: Long, v2: Long)) => List(Interval.closed(v1, v2))
      case Some(UnaryLogicalExpression(expression, _))            => extractTimeRange(Some(expression)).flatMap(i => ~i)
      case Some(TupledLogicalExpression(expression1, operator: TupledLogicalOperator, expression2: Expression)) =>
        val intervals = extractTimeRange(Some(expression1)) ++ extractTimeRange(Some(expression2))
        if (intervals.isEmpty)
          List.empty
        else
          operator match {
            case AndOperator =>
              List((extractTimeRange(Some(expression1)) ++ extractTimeRange(Some(expression2))).reduce(_ intersect _))
            case OrOperator =>
              List((extractTimeRange(Some(expression1)) ++ extractTimeRange(Some(expression2))).reduce(_ union _))
          }
      case _ => List.empty
    }
  }

  /**
    * Recursive method used to compute ranges for a temporal interval defined in where condition
    *
    * @param upperInterval interval upper bound, it's the previous one lower bound
    * @param lowerInterval interval lower bound, this value remained fixed during recursion
    * @param bucketSize range duration expressed in millis
    * @param acc result accumulator
    * @return
    */
  @tailrec
  def computeRangeForInterval(upperInterval: Long,
                              lowerInterval: Long,
                              bucketSize: Long,
                              acc: Seq[LongRange]): Seq[LongRange] = {
    val upperBound = (upperInterval.toDouble / bucketSize * bucketSize).toLong
    val lowerBound = upperBound - bucketSize
    if (lowerBound > lowerInterval && acc.size < NBuckets - 1) {
      computeRangeForInterval(lowerBound,
                              lowerInterval,
                              bucketSize,
                              acc :+ new LongRange(s"$lowerBound-$upperBound", lowerBound, true, upperBound, false))
    } else
      acc :+ new LongRange(s"$lowerBound-$upperBound", lowerBound, true, upperBound, false)
  }

  /**
    * Temporal buckets are computed as done in shards definition. So, given the time origin time buckets are computed
    * starting from actual timestamp going backward until limit is reached.
    *
    * @param rangeInterval
    * @return
    */
  def computeRanges(rangeInterval: Long,
                    whereCondition: Option[Condition],
                    location: Location,
                    now: Long): Seq[LongRange] = {

    val locationAsInterval                 = Interval.fromBounds(Closed(location.from), Closed(location.to))
    val timeIntervals: Seq[Interval[Long]] = TimeRangeExtractor.extractTimeRange(whereCondition.map(_.expression))

    timeIntervals match {
      case Nil =>
        val upperBound = location.to
        val lowerBound = location.from
        computeRangeForInterval(upperBound, lowerBound, rangeInterval, Seq.empty)
      case _ =>
        timeIntervals.filter(interval => interval.intersects(locationAsInterval)).flatMap { i =>
          val lowerBound = i.bottom(1).getOrElse(location.from)
          val upperBound = i.top(1).getOrElse(location.to)
          computeRangeForInterval(upperBound, lowerBound, rangeInterval, Seq.empty)
        }
    }
  }

  /**
    * Temporal buckets are computed as done in shards definition. So, given the time origin time buckets are computed
    * starting from actual timestamp going backward until limit is reached.
    *
    * @param rangeInterval
    * @param limit
    * @return
    */
  def computeRanges(rangeInterval: Long,
                    limit: Option[Int],
                    whereCondition: Option[Condition],
                    now: Long): Seq[LongRange] = {

    val timeIntervals: Seq[Interval[Long]] = TimeRangeExtractor.extractTimeRange(whereCondition.map(_.expression))

    timeIntervals match {
      case Nil =>
        val upperBound = now
        val lowerBound = upperBound - NBuckets * rangeInterval
        computeRangeForInterval(upperBound, lowerBound, rangeInterval, Seq.empty)
      case _ =>
        timeIntervals.flatMap { i =>
          val lowerBound = i.bottom(1).getOrElse(0L)
          val upperBound = i.top(1).getOrElse(now)
          computeRangeForInterval(upperBound, lowerBound, rangeInterval, Seq.empty)
        }
    }
  }

}
