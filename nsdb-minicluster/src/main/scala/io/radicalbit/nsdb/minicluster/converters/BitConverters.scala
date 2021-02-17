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

package io.radicalbit.nsdb.minicluster.converters

import io.radicalbit.nsdb.api.scala.{Namespace, Bit => ApiBit}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common._
import io.radicalbit.nsdb.rpc.common.{Dimension, Tag}
import io.radicalbit.nsdb.rpc.request.RPCInsert

/**
  * Object containing implicit class useful to convert [[Bit]] into [[ApiBit]] and vice versa.
  */
object BitConverters {

  implicit class BitConverter(bit: Bit) {

    def asApiBit(db: String, namespace: String, metric: String): ApiBit = {
      val initialBit = Namespace(db, namespace).metric(metric).timestamp(bit.timestamp)
      val apiBit = bit.value match {
        case NSDbLongType(v)   => initialBit.value(v)
        case NSDbDoubleType(v) => initialBit.value(v)
        case NSDbIntType(v)    => initialBit.value(v)
      }

      bit.dimensions.foreach {
        case (k, NSDbDoubleType(v)) => apiBit.dimension(k, v)
        case (k, NSDbLongType(v))   => apiBit.dimension(k, v)
        case (k, NSDbIntType(v))    => apiBit.dimension(k, v)
        case (k, NSDbStringType(v)) => apiBit.dimension(k, v)
      }

      bit.tags.foreach {
        case (k, NSDbDoubleType(v)) => apiBit.tag(k, v)
        case (k, NSDbLongType(v))   => apiBit.tag(k, v)
        case (k, NSDbIntType(v))    => apiBit.tag(k, v)
        case (k, NSDbStringType(v)) => apiBit.tag(k, v)
      }
      apiBit
    }
  }

  implicit class ApiBitConverter(bit: ApiBit) {
    private def valueFor(v: RPCInsert.Value): NSDbNumericType = v match {
      case _: RPCInsert.Value.DecimalValue => NSDbNumericType(v.decimalValue.get)
      case _: RPCInsert.Value.LongValue    => NSDbNumericType(v.longValue.get)
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

    def asCommonBit: Bit =
      Bit(
        timestamp = bit.timestamp.getOrElse(System.currentTimeMillis()),
        dimensions = bit.dimensions.collect {
          case (k, v) if !v.value.isStringValue || v.getStringValue != "" => (k, dimensionFor(v.value))
        }.toMap,
        tags = bit.tags.collect {
          case (k, v) if !v.value.isStringValue || v.getStringValue != "" => (k, tagFor(v.value))
        }.toMap,
        value = valueFor(bit.value)
      )
  }

}
