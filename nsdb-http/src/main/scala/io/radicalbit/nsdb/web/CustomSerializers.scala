package io.radicalbit.nsdb.web

import io.radicalbit.nsdb.common.statement.{Aggregation, AndOperator, AscOrderOperator, ComparisonOperator, CountAggregation, DescOrderOperator, GreaterOrEqualToOperator, GreaterThanOperator, LessOrEqualToOperator, LessThanOperator, LogicalOperator, MaxAggregation, MinAggregation, NotOperator, OrOperator, OrderOperator, SumAggregation}
import org.json4s.JsonAST.JField
import org.json4s.{CustomSerializer, JNull, JObject, JString}

object CustomSerializers {

  val customSerializers = List(AggregationSerializer, ComparisonOperatorSerializer, LogicalOperatorSerializer, OrderOperatorSerializer)
  
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

  case object OrderOperatorSerializer extends CustomSerializer[OrderOperator](ser = _ => ( {
    case JString(order) =>
      order match {
        case "asc" => AscOrderOperator("test")
        case "desc" => DescOrderOperator("test")
      }
    case JNull => null
  }, {
    case AscOrderOperator(order_by) => JObject(List(JField("order_by", JString(order_by)), JField("direction", JString("asc"))))
    case DescOrderOperator(order_by) => JObject(List(JField("order_by", JString(order_by)), JField("direction", JString("desc"))))
  }))

}
