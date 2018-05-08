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
import spire.implicits._
import spire.math.Interval
import spire.math.interval.{Closed, Open, Unbound}

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
}
