package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import org.scalatest.{Matchers, WordSpec}
import spire.math.Interval
import spire.math.interval.{Closed, Open, Unbound}
import spire.implicits._

class TimeRangeExtractorSpec extends WordSpec with Matchers {

  "A TimeRangeExtractor" when {

    "receive an expression that does not invole the timestamp" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(Some(
          RangeExpression(dimension = "other", value1 = 2L, value2 = 4L)
        )
        ) shouldBe List.empty
      }
    }

    "receive an expression containing a range selection" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(Some(
          RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L)
        )
        ) shouldBe List(
          Interval.closed(2L,4L)
        )
      }
    }

    "receive an expression containing a GTE selection" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(Some(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L),
        )
        ) shouldBe List(
          Interval.fromBounds(Closed(10L), Unbound())
        )
      }
    }

    "receive an expression containing a GT AND a LTE selection" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(Some(
          TupledLogicalExpression(
              expression1 = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = AndOperator,
              expression2 =
                ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
            )
        )
        ) shouldBe List(
        Interval.openLower(2,4)
        )
      }
    }

    "receive an expression containing a NOT condition" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(Some(
          UnaryLogicalExpression(
            expression = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
            operator = NotOperator
          )
        )
        ) shouldBe List(
          Interval.fromBounds(Unbound(), Closed(2))
        )

        TimeRangeExtractor.extractTimeRange(Some(
          UnaryLogicalExpression(
            expression = RangeExpression("timestamp", 2L,4L),
            operator = NotOperator
          )
        )
        ) shouldBe List(
          Interval.fromBounds(Unbound(), Open(2)),
          Interval.fromBounds(Open(4), Unbound())
        )
      }
    }

    "receive an expression containing a GTE OR a LT selection" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(Some(
          UnaryLogicalExpression(
          expression = TupledLogicalExpression(
            expression1 =
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
            operator = OrOperator,
            expression2 = ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
          ),
          operator = NotOperator)
        )
        )shouldBe List(
          Interval.fromBounds(Unbound(), Open(0))
        )
      }
    }

    "receive an expression involving the timestamp and another irrelevant" should {
      "parse it successfully" in {
        TimeRangeExtractor.extractTimeRange(Some(
          TupledLogicalExpression(
            expression1 =
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
            operator = OrOperator,
            expression2 = EqualityExpression(dimension = "name", value = "john")
          )
        )
        ) shouldBe List(
        Interval.fromBounds(Closed(2l), Unbound())
        )

        TimeRangeExtractor.extractTimeRange(Some(
          TupledLogicalExpression(
            expression1 =
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
            operator = AndOperator,
            expression2 = EqualityExpression(dimension = "name", value = "john")
          )
        )
        ) shouldBe List(
          Interval.fromBounds(Closed(2l), Unbound())
        )
      }
    }

  }
}
