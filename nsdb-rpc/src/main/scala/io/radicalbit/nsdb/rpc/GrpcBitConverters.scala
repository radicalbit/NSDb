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

package io.radicalbit.nsdb.rpc

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common._
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
          case NSDbLongType(v)   => GrpcBit.Value.LongValue(v)
          case NSDbDoubleType(v) => GrpcBit.Value.DecimalValue(v)
          case NSDbIntType(v)    => GrpcBit.Value.LongValue(v.longValue())
        },
        dimensions = bit.dimensions.map {
          case (k, NSDbDoubleType(v)) => (k, Dimension(Dimension.Value.DecimalValue(v)))
          case (k, NSDbLongType(v))   => (k, Dimension(Dimension.Value.LongValue(v)))
          case (k, NSDbIntType(v))    => (k, Dimension(Dimension.Value.LongValue(v.longValue())))
          case (k, NSDbStringType(v)) => (k, Dimension(Dimension.Value.StringValue(v)))
          case (k, v)                 => (k, Dimension(Dimension.Value.StringValue(v.toString)))
        },
        tags = bit.tags.map {
          case (k, NSDbDoubleType(v)) => (k, Tag(Tag.Value.DecimalValue(v)))
          case (k, NSDbLongType(v))   => (k, Tag(Tag.Value.LongValue(v)))
          case (k, NSDbIntType(v))    => (k, Tag(Tag.Value.LongValue(v.longValue())))
          case (k, NSDbStringType(v)) => (k, Tag(Tag.Value.StringValue(v)))
          case (k, v)                 => (k, Tag(Tag.Value.StringValue(v.toString)))
        }
      )
  }

  implicit class GrpcBitConverter(bit: GrpcBit) {
    private def valueFor(v: GrpcBit.Value): NSDbNumericType = v match {
      case _: GrpcBit.Value.DecimalValue => NSDbNumericType(v.decimalValue.get)
      case _: GrpcBit.Value.LongValue    => NSDbNumericType(v.longValue.get)
    }

    private def dimensionFor(v: Dimension.Value): NSDbType = v match {
      case _: Dimension.Value.DecimalValue => NSDbType(v.decimalValue.get)
      case _: Dimension.Value.LongValue    => NSDbType(v.longValue.get)
      case _                               => NSDbType(v.stringValue.get)
    }

    private def tagFor(v: Tag.Value): NSDbType = v match {
      case _: Tag.Value.DecimalValue => NSDbType(v.decimalValue.get)
      case _: Tag.Value.LongValue    => NSDbType(v.longValue.get)
      case _                         => NSDbType(v.stringValue.get)
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
