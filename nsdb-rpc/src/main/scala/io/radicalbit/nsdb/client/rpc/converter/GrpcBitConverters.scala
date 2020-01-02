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

package io.radicalbit.nsdb.client.rpc.converter
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.rpc.common.{Dimension, Tag, Bit => GrpcBit}

/**
  * Object containing convenience methods to convert from common bit to grpc bit and vice versa
  */
object GrpcBitConverters {

  implicit class BitConverter(bit: Bit) {
    def asGrpcBit: GrpcBit =
      GrpcBit(
        timestamp = bit.timestamp,
        value = bit.value match {
          case v: java.lang.Long                         => GrpcBit.Value.LongValue(v)
          case v: java.lang.Double                       => GrpcBit.Value.DecimalValue(v)
          case v: java.lang.Float                        => GrpcBit.Value.DecimalValue(v.doubleValue())
          case v: java.lang.Integer                      => GrpcBit.Value.LongValue(v.longValue())
          case v: java.math.BigDecimal if v.scale() == 0 => GrpcBit.Value.LongValue(v.longValue())
          case v: java.math.BigDecimal                   => GrpcBit.Value.DecimalValue(v.doubleValue())
        },
        dimensions = bit.dimensions.map {
          case (k, v: java.lang.Double)  => (k, Dimension(Dimension.Value.DecimalValue(v)))
          case (k, v: java.lang.Float)   => (k, Dimension(Dimension.Value.DecimalValue(v.doubleValue())))
          case (k, v: java.lang.Long)    => (k, Dimension(Dimension.Value.LongValue(v)))
          case (k, v: java.lang.Integer) => (k, Dimension(Dimension.Value.LongValue(v.longValue())))
          case (k, v: java.math.BigDecimal) if v.scale() == 0 =>
            (k, Dimension(Dimension.Value.LongValue(v.longValue())))
          case (k, v: java.math.BigDecimal) => (k, Dimension(Dimension.Value.DecimalValue(v.doubleValue())))
          case (k, v)                       => (k, Dimension(Dimension.Value.StringValue(v.toString)))
        },
        tags = bit.tags.map {
          case (k, v: java.lang.Double)  => (k, Tag(Tag.Value.DecimalValue(v)))
          case (k, v: java.lang.Float)   => (k, Tag(Tag.Value.DecimalValue(v.doubleValue())))
          case (k, v: java.lang.Long)    => (k, Tag(Tag.Value.LongValue(v)))
          case (k, v: java.lang.Integer) => (k, Tag(Tag.Value.LongValue(v.longValue())))
          case (k, v: java.math.BigDecimal) if v.scale() == 0 =>
            (k, Tag(Tag.Value.LongValue(v.longValue())))
          case (k, v: java.math.BigDecimal) => (k, Tag(Tag.Value.DecimalValue(v.doubleValue())))
          case (k, v)                       => (k, Tag(Tag.Value.StringValue(v.toString)))
        }
      )
  }

  implicit class GrpcBitConverter(bit: GrpcBit) {
    private def valueFor(v: GrpcBit.Value): JSerializable = v match {
      case _: GrpcBit.Value.DecimalValue => v.decimalValue.get
      case _: GrpcBit.Value.LongValue    => v.longValue.get
    }

    private def dimensionFor(v: Dimension.Value): JSerializable = v match {
      case _: Dimension.Value.DecimalValue => v.decimalValue.get
      case _: Dimension.Value.LongValue    => v.longValue.get
      case _                               => v.stringValue.get
    }

    private def tagFor(v: Tag.Value): JSerializable = v match {
      case _: Tag.Value.DecimalValue => v.decimalValue.get
      case _: Tag.Value.LongValue    => v.longValue.get
      case _                         => v.stringValue.get
    }

    def asBit: Bit =
      Bit(
        timestamp = bit.timestamp,
        dimensions = bit.dimensions.collect {
          case (k, v) if !v.value.isStringValue || v.getStringValue != "" => (k, dimensionFor(v.value))
        },
        tags = bit.tags.collect {
          case (k, v) if !v.value.isStringValue || v.getStringValue != "" => (k, tagFor(v.value))
        },
        value = valueFor(bit.value)
      )
  }

}
