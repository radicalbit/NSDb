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

import io.radicalbit.nsdb.common.protocol.Coordinates
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.{Location, TimeRange}
import org.scalatest.{Matchers, WordSpec}
import spire.math.Interval
import spire.math.interval.{Closed, Open, Unbound}
import spire.implicits._

class TimeRangeExtractorSpec extends WordSpec with Matchers {

  "A TimeRangeExtractor" when {

    "receive a simple expression that does not involve the timestamp" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(
          Some(
            RangeExpression(dimension = "other", value1 = 2L, value2 = 4L)
          )) shouldBe List.empty
      }

      "parse it successfully with and operator" in {
        TimeRangeExtractor.extractTimeRange(
          Some(
            TupledLogicalExpression(
              expression1 = RangeExpression(dimension = "other", value1 = 2L, value2 = 4L),
              operator = AndOperator,
              expression2 = RangeExpression(dimension = "other2", value1 = 2L, value2 = 4L)
            )
          )) shouldBe List.empty
      }

      "parse it successfully with or operator" in {
        TimeRangeExtractor.extractTimeRange(
          Some(
            TupledLogicalExpression(
              expression1 = RangeExpression(dimension = "other", value1 = 2L, value2 = 4L),
              operator = OrOperator,
              expression2 = RangeExpression(dimension = "other2", value1 = 2L, value2 = 4L)
            )
          )) shouldBe List.empty
      }
    }

    "receive a simple expression that does involve the timestamp" should {
      "parse it successfully in case of a range selection" in {
        TimeRangeExtractor.extractTimeRange(
          Some(
            RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L)
          )) shouldBe List(
          Interval.closed(2L, 4L)
        )
      }

      "parse it successfully in case of a GTE selection" in {
        TimeRangeExtractor.extractTimeRange(
          Some(ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))
        ) shouldBe List(
          Interval.fromBounds(Closed(10L), Unbound())
        )
      }

      "parse it successfully in case of a GT AND a LTE selection" in {
        TimeRangeExtractor.extractTimeRange(
          Some(
            TupledLogicalExpression(
              expression1 = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = AndOperator,
              expression2 =
                ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
            )
          )) shouldBe List(
          Interval.openLower(2, 4)
        )
      }

      "parse it successfully of a NOT condition" in {
        TimeRangeExtractor.extractTimeRange(
          Some(
            UnaryLogicalExpression(
              expression = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = NotOperator
            )
          )) shouldBe List(
          Interval.fromBounds(Unbound(), Closed(2))
        )

        TimeRangeExtractor.extractTimeRange(
          Some(
            UnaryLogicalExpression(
              expression = RangeExpression("timestamp", 2L, 4L),
              operator = NotOperator
            )
          )) shouldBe List(
          Interval.fromBounds(Unbound(), Open(2)),
          Interval.fromBounds(Open(4), Unbound())
        )
      }

      "parse it successfully in case of a GTE OR a LT selection" in {
        TimeRangeExtractor.extractTimeRange(
          Some(
            UnaryLogicalExpression(
              expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
              ),
              operator = NotOperator
            )
          )) shouldBe List(
          Interval.fromBounds(Unbound(), Open(0))
        )
      }
    }

    "receive an expression involving the timestamp and another irrelevant" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(
          Some(
            TupledLogicalExpression(
              expression1 =
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
              operator = OrOperator,
              expression2 = EqualityExpression(dimension = "name", value = "john")
            )
          )) shouldBe List(
          Interval.fromBounds(Closed(2l), Unbound())
        )

        TimeRangeExtractor.extractTimeRange(
          Some(
            TupledLogicalExpression(
              expression1 =
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
              operator = AndOperator,
              expression2 = EqualityExpression(dimension = "name", value = "john")
            )
          )) shouldBe List(
          Interval.fromBounds(Closed(2l), Unbound())
        )
      }
    }

    "executing computeRangesForLocation " should {
      "return a seq of ranges" in {
        val res = TimeRangeExtractor.computeRangesForLocation(5L, None, Location("whatever", "whatever", 0L, 10L))
        res shouldBe Seq(TimeRange(5, 10, false, true), TimeRange(0, 5, true, true))
      }

      "return a seq of ranges in case range length is not a divisor of the location length" in {
        val res = TimeRangeExtractor.computeRangesForLocation(3L, None, Location("whatever", "whatever", 0L, 10L))
        res shouldBe Seq(TimeRange(7, 10, false, true),
                         TimeRange(4, 7, false, true),
                         TimeRange(1, 4, false, true),
                         TimeRange(0, 1, true, true))
      }

      "return a single range in case range length is greater than the location" in {
        val res = TimeRangeExtractor.computeRangesForLocation(30L, None, Location("whatever", "whatever", 0L, 10L))
        res shouldBe Seq(TimeRange(0, 10, true, true))
      }

      "return a seq of ranges for a RangeExpression" in {
        val res = TimeRangeExtractor.computeRangesForLocation(
          5L,
          Some(Condition(RangeExpression(dimension = "timestamp", value1 = 0L, value2 = 5L))),
          Location("whatever", "whatever", 0L, 10L))
        res shouldBe Seq(TimeRange(0, 5, true, true))
      }

      "return a seq of ranges for a left bounded interval(>=) lower than now " in {
        val res = TimeRangeExtractor.computeRangesForLocation(
          5L,
          Some(
            Condition(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 50L))),
          Location("whatever", "whatever", 0L, 100L))

        res shouldBe Seq(
          TimeRange(95, 100, false, true),
          TimeRange(90, 95, false, true),
          TimeRange(85, 90, false, true),
          TimeRange(80, 85, false, true),
          TimeRange(75, 80, false, true),
          TimeRange(70, 75, false, true),
          TimeRange(65, 70, false, true),
          TimeRange(60, 65, false, true),
          TimeRange(55, 60, false, true),
          TimeRange(50, 55, true, true)
        )
      }

      "return a seq of ranges for a right bounded interval(<=) lower than now " in {
        val res = TimeRangeExtractor.computeRangesForLocation(
          5L,
          Some(
            Condition(ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 50L))),
          Location("whatever", "whatever", 0L, 100L))

        res shouldBe Seq(
          TimeRange(45, 50, false, true),
          TimeRange(40, 45, false, true),
          TimeRange(35, 40, false, true),
          TimeRange(30, 35, false, true),
          TimeRange(25, 30, false, true),
          TimeRange(20, 25, false, true),
          TimeRange(15, 20, false, true),
          TimeRange(10, 15, false, true),
          TimeRange(5, 10, false, true),
          TimeRange(0, 5, true, true)
        )
      }

      "return a seq of ranges for both right and left bounded interval( >= && <=) " in {
        val res = TimeRangeExtractor.computeRangesForLocation(
          5L,
          Some(
            Condition(TupledLogicalExpression(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 70L),
              AndOperator,
              ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 90L)
            ))),
          Location("whatever", "whatever", 0L, 100L)
        )

        res shouldBe Seq(
          TimeRange(85, 90, false, true),
          TimeRange(80, 85, false, true),
          TimeRange(75, 80, false, true),
          TimeRange(70, 75, true, true)
        )
      }

      "return a seq of ranges for both right and left bounded interval( > && <) " in {
        val res = TimeRangeExtractor.computeRangesForLocation(
          5L,
          Some(
            Condition(TupledLogicalExpression(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 70L),
              AndOperator,
              ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 90L)
            ))),
          Location("whatever", "whatever", 0L, 100L)
        )

        res shouldBe Seq(
          TimeRange(84, 89, false, true),
          TimeRange(79, 84, false, true),
          TimeRange(74, 79, false, true),
          TimeRange(71, 74, true, true)
        )
      }
    }

    "executing getLocationsToEvict" should {

      "filter locations given a threshold" in {
        val locationSequence = Seq(
          Location("metric", "node", 0, 5),
          Location("metric", "node", 6, 10),
          Location("metric", "node", 11, 15),
          Location("metric", "node", 16, 20),
          Location("metric", "node", 21, 25)
        )

        TimeRangeExtractor.getLocationsToEvict(locationSequence, 15) shouldBe (
          Seq(Location("metric", "node", 0, 5), Location("metric", "node", 6, 10)),
          Seq(Location("metric", "node", 11, 15))
        )

      }

    }
  }

}
