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

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common._
import org.json4s.JsonAST.{JDouble, JField, JInt, JLong}
import org.json4s.{CustomSerializer, JObject, JString, JsonAST}

object BitSerializer {

  private def extractJValue(nsdbType: NSDbType): JsonAST.JValue = {
    nsdbType match {
      case NSDbDoubleType(rawValue) => JDouble(rawValue)
      case NSDbIntType(rawValue)    => JInt(rawValue)
      case NSDbLongType(rawValue)   => JLong(rawValue)
      case NSDbStringType(rawValue) => JString(rawValue)
    }
  }

  case object BitSerializer
      extends CustomSerializer[Bit](
        _ =>
          (
            {
              case _ => ???
            }, {
              case bit: Bit =>
                JObject(
                  List(
                    JField("timestamp", JLong(bit.timestamp)),
                    JField("value", extractJValue(bit.value)),
                    JField("dimensions",
                           JObject(bit.dimensions.map { case (k, v)   => JField(k, extractJValue(v)) }.toList)),
                    JField("tags", JObject(bit.tags.map { case (k, v) => JField(k, extractJValue(v)) }.toList))
                  )
                )
            }
        ))

}
