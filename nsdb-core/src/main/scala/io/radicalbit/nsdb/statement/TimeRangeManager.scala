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

package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.{Location, TimeRange}
import spire.implicits._
import spire.math.Interval
import spire.math.interval.{Closed, Open, Unbound}

import scala.annotation.tailrec

/**
  * Provides a utility method to retrieve a list of [[Interval]] from a given [[Expression]]. That interval list will be used to identify shards to execute query against.
  */
object TimeRangeManager {

  def extractTimeRange(exp: Option[Expression]): List[Interval[Long]] = {
    exp match {
      case Some(EqualityExpression("timestamp", ComparisonValue(value: Long))) => List(Interval.point(value))
      case Some(ComparisonExpression("timestamp", operator: ComparisonOperator, ComparisonValue(value: Long))) =>
        operator match {
          case GreaterThanOperator      => List(Interval.fromBounds(Open(value), Unbound()))
          case GreaterOrEqualToOperator => List(Interval.fromBounds(Closed(value), Unbound()))
          case LessThanOperator         => List(Interval.openUpper(0, value))
          case LessOrEqualToOperator    => List(Interval.closed(0, value))
        }
      case Some(RangeExpression("timestamp", ComparisonValue(v1: Long), ComparisonValue(v2: Long))) =>
        List(Interval.closed(v1, v2))
      case Some(UnaryLogicalExpression(expression)) => extractTimeRange(Some(expression)).flatMap(i => ~i)
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
    * Recursive method used to compute ranges for a temporal interval
    *
    * @param upperInterval interval upper bound, it's the previous one lower bound
    * @param lowerInterval interval lower bound, this value remained fixed during recursion
    * @param rangeLength range duration expressed in millis
    * @param acc result accumulator
    * @return
    */
  @tailrec
  private def computeRangeForInterval(upperInterval: Long,
                                      lowerInterval: Long,
                                      rangeLength: Long,
                                      acc: Seq[TimeRange]): Seq[TimeRange] = {

    val lowerBound = upperInterval - rangeLength

    if (lowerBound <= lowerInterval)
      acc :+ TimeRange(lowerInterval, upperInterval, lowerInclusive = true, upperInclusive = true)
    else
      computeRangeForInterval(
        lowerBound,
        lowerInterval,
        rangeLength,
        acc :+ TimeRange(lowerBound, upperInterval, lowerInclusive = false, upperInclusive = true))
  }

  /**
    * Temporal buckets are computed given the overall time interval and a where condition
    * starting from actual timestamp going backward until the limit set by the condition is reached.
    *
    * @param upperInterval interval upper bound, it's the previous one lower bound
    * @param lowerInterval interval lower bound, this value remained fixed during recursion
    * @param rangeLength The range length in milliseconds.
    * @param whereCondition a where condition extracted from a SQL query used to determine ranges limits.
    * @return A sequence of [[TimeRange]] for the given input params.
    */
  def computeRangesForIntervalAndCondition(upperInterval: Long,
                                           lowerInterval: Long,
                                           rangeLength: Long,
                                           whereCondition: Option[Condition]): Seq[TimeRange] = {

    val globalInterval                     = Interval.fromBounds(Closed(lowerInterval), Closed(upperInterval))
    val timeIntervals: Seq[Interval[Long]] = TimeRangeManager.extractTimeRange(whereCondition.map(_.expression))

    timeIntervals match {
      case Nil =>
        computeRangeForInterval(upperInterval, lowerInterval, rangeLength, Seq.empty)
      case _ =>
        timeIntervals.filter(interval => interval.intersects(globalInterval)).flatMap { i =>
          val upperBound = i.top(1).getOrElse(upperInterval)
          val lowerBound = i.bottom(1).getOrElse(upperInterval)
          computeRangeForInterval(upperBound, lowerBound, rangeLength, Seq.empty)
        }
    }
  }

  /**
    * Filters a sequence of [[Location]] given a threshold.
    * Locations that are older that the threshold are returned.
    * @param locations the location with coordinates to evict.
    * @param threshold the lowest timestamp to keep.
    * @return the location that must be completely evicted, and the locations that must be partially evicted.
    */
  def getLocationsToEvict(locations: Seq[Location], threshold: Long): (Seq[Location], Seq[Location]) = {
    (locations.filter { l =>
      l.to < threshold
    }, locations.filter { l =>
      l.from <= threshold && l.to >= threshold
    })
  }
}
