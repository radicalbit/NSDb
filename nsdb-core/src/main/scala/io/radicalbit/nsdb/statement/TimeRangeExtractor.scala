package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import spire.implicits._
import spire.math.Interval
import spire.math.interval.{Closed, Open, Unbound}

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
        operator match {
          case AndOperator =>
            List(
              (extractTimeRange(Some(expression1)) ++ extractTimeRange(Some(expression2)))
                .reduce(_ intersect _))
          case OrOperator =>
            List(
              (extractTimeRange(Some(expression1)) ++
                extractTimeRange(Some(expression2))).reduce(_ union _)
            )
        }
      case _ => List.empty
    }
  }
}
