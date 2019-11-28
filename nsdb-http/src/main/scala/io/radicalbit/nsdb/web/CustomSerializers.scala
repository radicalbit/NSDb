package io.radicalbit.nsdb.web

import io.radicalbit.nsdb.common.statement._
import org.json4s
import org.json4s.JsonAST.{JArray, JDouble, JField, JInt, JLong, JValue}
import org.json4s.{CustomSerializer, JNull, JObject, JString}


object CustomSerializers {

  val customSerializers = List(AggregationSerializer, ComparisonOperatorSerializer, LogicalOperatorSerializer, OrderOperatorSerializer, LikeExpressionSerializer, EqualityExpressionSerializer)

  case object AggregationSerializer extends CustomSerializer[Aggregation](_ => ({
        case JString(aggregation) =>
          aggregation match {
            case "count" => CountAggregation
            case "max" => MaxAggregation
            case "min" => MinAggregation
            case "sum" => SumAggregation
          }
        case JNull => null
      }, {
        case CountAggregation => JString("count")
        case MaxAggregation => JString("max")
        case MinAggregation => JString("min")
        case SumAggregation => JString("sum")
      }))

  case object ComparisonOperatorSerializer extends CustomSerializer[ComparisonOperator](_ => ({
    case JString(comparison) =>
      comparison match {
        case ">" => GreaterThanOperator
        case ">=" => GreaterOrEqualToOperator
        case "<" => LessThanOperator
        case "<=" => LessOrEqualToOperator
      }
    case JNull => null
  }, {
    case GreaterThanOperator => JString(">")
    case GreaterOrEqualToOperator => JString(">=")
    case LessThanOperator => JString("<")
    case LessOrEqualToOperator => JString("<=")
  }))

  case object LogicalOperatorSerializer extends CustomSerializer[LogicalOperator](_ => ({
    case JString(logical) =>
      logical match {
        case "not" => NotOperator
        case "and" => AndOperator
        case "or" => OrOperator
      }
    case JNull => null
  }, {
    case NotOperator => JString("not")
    case AndOperator => JString("and")
    case OrOperator => JString("or")
  }))

  case object OrderOperatorSerializer extends CustomSerializer[OrderOperator](_ => ( {
    case JObject(List(JField("order_by", JString(order)), JField("direction", JString(direction)))) =>
      direction match {
        case "asc" => AscOrderOperator(order)
        case "desc" => DescOrderOperator(order)
      }
    case JNull => null
  }, {
    case AscOrderOperator(order_by) => JObject(List(JField("order_by", JString(order_by)), JField("direction", JString("asc"))))
    case DescOrderOperator(order_by) => JObject(List(JField("order_by", JString(order_by)), JField("direction", JString("desc"))))
  }))

  case object NullableExpressionSerializer extends CustomSerializer[NullableExpression](_ => ( {
    case JObject(List(JField(_, JString(dimension)), JField(_, JString("like") ))) => NullableExpression(dimension)
  }, {
    case NullableExpression(dimension) => JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("null"))))
  }
    ))

  case object LikeExpressionSerializer extends CustomSerializer[LikeExpression](_ => ( {
    case JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("null")), JField("value", JString(value)))) => LikeExpression(dimension, value)
  }, {
    case LikeExpression(dimension, value) => JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("like")), JField("value", JString(value))))
  }
  ))

  case object EqualityExpressionSerializer extends CustomSerializer[EqualityExpression[_]](_ => ( {
    case JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JLong(value)))) => EqualityExpression(dimension, AbsoluteComparisonValue(value: Long))
    case JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JInt(value)))) =>EqualityExpression(dimension, AbsoluteComparisonValue(value.intValue(): Int))
    case JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JString(value)))) =>EqualityExpression(dimension, AbsoluteComparisonValue(value: String))
    case JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JDouble(value)))) =>EqualityExpression(dimension, AbsoluteComparisonValue(value: Double))
    case JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JObject(
    List(JField("value", JLong(value)), JField("operator", JString(operator)), JField("quantity", JLong(quantity)), JField("unitMeasure", JString(unitMeasure)))
    )))) => EqualityExpression(dimension, RelativeComparisonValue(value: Long, operator, quantity: Long, unitMeasure))
  }, {
    case EqualityExpression(dimension, AbsoluteComparisonValue(value: Long)) => JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JLong(value))))
    case EqualityExpression(dimension, AbsoluteComparisonValue(value: Int)) => JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JInt(value))))
    case EqualityExpression(dimension, AbsoluteComparisonValue(value: String)) => JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JString(value))))
    case EqualityExpression(dimension, AbsoluteComparisonValue(value: Double)) => JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JDouble(value))))
    case EqualityExpression(dimension, RelativeComparisonValue(value: Long, operator, quantity: Long, unitMeasure)) =>
      JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("=")), JField("value", JObject(
        List(JField("value", JLong(value)), JField("operator", JString(operator)), JField("quantity", JLong(quantity)), JField("unitMeasure", JString(unitMeasure)))
      ))))
  }))

}
