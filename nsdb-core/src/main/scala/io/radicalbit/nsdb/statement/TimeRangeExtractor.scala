package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import spire.implicits._
import spire.math.Interval
import spire.math.interval.{Closed, Open, Unbound}

object TimeRangeExtractor {
  def extractTimeRange(exp: Expression): List[Interval[Long]] = {
    exp match {
      case EqualityExpression("timestamp", value: Long) => List(Interval.point(value))
      case ComparisonExpression("timestamp", operator: ComparisonOperator, value: Long) =>
        operator match {
          case GreaterThanOperator      => List(Interval.fromBounds(Open(value), Unbound()))
          case GreaterOrEqualToOperator => List(Interval.fromBounds(Closed(value), Unbound()))
          case LessThanOperator         => List(Interval.openUpper(0, value))
          case LessOrEqualToOperator    => List(Interval.closed(0, value))
        }
      case RangeExpression("timestamp", v1: Long, v2: Long) => List(Interval.closed(v1, v2))
      case UnaryLogicalExpression(expression, _)            => extractTimeRange(expression).flatMap(i => ~i)
      case TupledLogicalExpression(expression1, operator: TupledLogicalOperator, expression2: Expression) =>
        operator match {
          case AndOperator =>
            List(
              (extractTimeRange(expression1) ++ extractTimeRange(expression2))
                .reduce(_ intersect _))
          case OrOperator =>
            List(
              (extractTimeRange(expression1) ++
                extractTimeRange(expression2)).reduce(_ union _) //.getOrElse(Interval.empty[Long])))
            )
        }
      case _ => List.empty
    }
  }
}
