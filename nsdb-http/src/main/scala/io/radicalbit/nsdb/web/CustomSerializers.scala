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

package io.radicalbit.nsdb.web

import io.radicalbit.nsdb.common._
import io.radicalbit.nsdb.common.statement._
import org.json4s.JsonAST.{JDouble, JField, JInt, JLong}
import org.json4s.{CustomSerializer, JNull, JObject, JString}

object CustomSerializers {

  val customSerializers = List(
    NSDbTypeSerializer,
    AggregationSerializer,
    ComparisonOperatorSerializer,
    LogicalOperatorSerializer,
    OrderOperatorSerializer,
    NullableExpressionSerializer,
    LikeExpressionSerializer,
    EqualityExpressionSerializer
  )

  case object NSDbTypeSerializer
      extends CustomSerializer[NSDbType](_ =>
        ({
          case JDouble(value) => NSDbDoubleType(value)
          case JInt(value)    => NSDbIntType(value.intValue)
          case JLong(value)   => NSDbLongType(value)
          case JString(value) => NSDbStringType(value)
        }, {
          case NSDbDoubleType(value) => JDouble(value)
          case NSDbIntType(value)    => JInt(value)
          case NSDbLongType(value)   => JLong(value)
          case NSDbStringType(value) => JString(value)
        }))

  case object AggregationSerializer
      extends CustomSerializer[Aggregation](_ =>
        ({
          case JString(aggregation) =>
            aggregation.toLowerCase match {
              case "count" => CountAggregation
              case "max"   => MaxAggregation
              case "min"   => MinAggregation
              case "sum"   => SumAggregation
            }
          case JNull => null
        }, {
          case CountAggregation => JString("count")
          case MaxAggregation   => JString("max")
          case MinAggregation   => JString("min")
          case SumAggregation   => JString("sum")
        }))

  case object ComparisonOperatorSerializer
      extends CustomSerializer[ComparisonOperator](_ =>
        ({
          case JString(comparison) =>
            comparison match {
              case ">"  => GreaterThanOperator
              case ">=" => GreaterOrEqualToOperator
              case "<"  => LessThanOperator
              case "<=" => LessOrEqualToOperator
            }
          case JNull => null
        }, {
          case GreaterThanOperator      => JString(">")
          case GreaterOrEqualToOperator => JString(">=")
          case LessThanOperator         => JString("<")
          case LessOrEqualToOperator    => JString("<=")
        }))

  case object LogicalOperatorSerializer
      extends CustomSerializer[LogicalOperator](_ =>
        ({
          case JString(logical) =>
            logical.toLowerCase match {
              case "and" => AndOperator
              case "or"  => OrOperator
              case "not" => NotOperator
            }
          case JNull => null
        }, {
          case AndOperator => JString("and")
          case OrOperator  => JString("or")
          case NotOperator => JString("not")
        }))

  case object OrderOperatorSerializer
      extends CustomSerializer[OrderOperator](_ =>
        ({
          case JObject(List(JField("order_by", JString(order)), JField("direction", JString(direction)))) =>
            direction.toLowerCase match {
              case "asc"  => AscOrderOperator(order)
              case "desc" => DescOrderOperator(order)
            }
          case JNull => null
        }, {
          case AscOrderOperator(orderBy) =>
            JObject(List(JField("order_by", JString(orderBy)), JField("direction", JString("asc"))))
          case DescOrderOperator(orderBy) =>
            JObject(List(JField("order_by", JString(orderBy)), JField("direction", JString("desc"))))
        }))

  case object NullableExpressionSerializer
      extends CustomSerializer[NullableExpression](_ =>
        ({
          case JObject(List(JField(_, JString(dimension)), JField(_, JString("null")))) => NullableExpression(dimension)
        }, {
          case NullableExpression(dimension) =>
            JObject(List(JField("dimension", JString(dimension)), JField("comparison", JString("null"))))
        }))

  case object LikeExpressionSerializer
      extends CustomSerializer[LikeExpression](_ =>
        ({
          case JObject(
              List(JField("dimension", JString(dimension)),
                   JField("comparison", JString("like")),
                   JField("value", JString(value)))) =>
            LikeExpression(dimension, value)
        }, {
          case LikeExpression(dimension, value) =>
            JObject(
              List(JField("dimension", JString(dimension)),
                   JField("comparison", JString("like")),
                   JField("value", JString(value.rawValue.toString))))
        }))

  case object EqualityExpressionSerializer
      extends CustomSerializer[EqualityExpression](_ =>
        ({
          case _ => throw new UnsupportedOperationException("Deserializing an EqualityExpression is not yet supported")
        }, {
          case EqualityExpression(dimension, AbsoluteComparisonValue(NSDbLongType(value))) =>
            JObject(
              List(JField("dimension", JString(dimension)),
                   JField("comparison", JString("=")),
                   JField("value", JLong(value))))
          case EqualityExpression(dimension, AbsoluteComparisonValue(NSDbIntType(value))) =>
            JObject(
              List(JField("dimension", JString(dimension)),
                   JField("comparison", JString("=")),
                   JField("value", JInt(value))))
          case EqualityExpression(dimension, AbsoluteComparisonValue(NSDbStringType(value))) =>
            JObject(
              List(JField("dimension", JString(dimension)),
                   JField("comparison", JString("=")),
                   JField("value", JString(value))))
          case EqualityExpression(dimension, AbsoluteComparisonValue(NSDbDoubleType(value))) =>
            JObject(
              List(JField("dimension", JString(dimension)),
                   JField("comparison", JString("=")),
                   JField("value", JDouble(value))))
          case EqualityExpression(dimension, RelativeComparisonValue(operator, quantity, unitMeasure)) =>
            JObject(
              List(
                JField("dimension", JString(dimension)),
                JField("comparison", JString("=")),
                JField(
                  "value",
                  JObject(
                    List(JField("operator", JString(operator)),
                         JField("quantity", JLong(quantity)),
                         JField("unitMeasure", JString(unitMeasure)))
                  )
                )
              ))
        }))
}
