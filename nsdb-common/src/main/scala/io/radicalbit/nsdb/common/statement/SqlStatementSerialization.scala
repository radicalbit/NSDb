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

package io.radicalbit.nsdb.common.statement

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

object SqlStatementSerialization {

  object ComparisonOperatorSerialization {

    class ComparisonOperatorJsonSerializer extends StdSerializer[ComparisonOperator](classOf[ComparisonOperator]) {

      override def serialize(value: ComparisonOperator, gen: JsonGenerator, provider: SerializerProvider): Unit =
        gen.writeString(value.toString)
    }

    class ComparisonOperatorJsonDeserializer extends StdDeserializer[ComparisonOperator](classOf[ComparisonOperator]) {

      override def deserialize(p: JsonParser, ctxt: DeserializationContext): ComparisonOperator = {
        p.getText match {
          case "GreaterThanOperator"      => GreaterThanOperator
          case "GreaterOrEqualToOperator" => GreaterOrEqualToOperator
          case "LessThanOperator"         => LessThanOperator
          case "LessOrEqualToOperator"    => LessOrEqualToOperator
        }
      }
    }

  }

  object AggregationSerialization {

    class AggregationJsonSerializer extends StdSerializer[Aggregation](classOf[Aggregation]) {

      override def serialize(value: Aggregation, gen: JsonGenerator, provider: SerializerProvider): Unit =
        gen.writeString(value.toString)
    }

    class AggregationJsonDeserializer extends StdDeserializer[Aggregation](classOf[Aggregation]) {

      override def deserialize(p: JsonParser, ctxt: DeserializationContext): Aggregation = {
        p.getText match {
          case "CountAggregation" => CountAggregation
          case "MaxAggregation"   => MaxAggregation
          case "MinAggregation"   => MinAggregation
          case "SumAggregation"   => SumAggregation
        }
      }
    }

  }

  object LogicalOperatorSerialization {

    class LogicalOperatorJsonSerializer extends StdSerializer[LogicalOperator](classOf[LogicalOperator]) {

      override def serialize(value: LogicalOperator, gen: JsonGenerator, provider: SerializerProvider): Unit =
        gen.writeString(value.toString)
    }

    class LogicalOperatorJsonDeserializer extends StdDeserializer[LogicalOperator](classOf[LogicalOperator]) {

      override def deserialize(p: JsonParser, ctxt: DeserializationContext): LogicalOperator = {
        p.getText match {
          case "NotOperator"   => NotOperator
          case "AndOperator"   => AndOperator
          case "OrAggregation" => OrOperator
        }
      }
    }

  }
}
