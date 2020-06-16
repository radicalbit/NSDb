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

import io.radicalbit.nsdb.common.statement.RelativeComparisonValue
import org.json4s.{CustomSerializer, JObject, JString}
import org.json4s.JsonAST.{JField, JInt, JLong}

case object CustomSerializerForTest
    extends CustomSerializer[RelativeComparisonValue](_ =>
      ({
        case JObject(
            List(JField("value", JLong(0L)),
                 JField("operator", JString(operator)),
                 JField("quantity", JInt(quantity)),
                 JField("unitMeasure", JString(unitMeasure)))) =>
          RelativeComparisonValue(0L, operator, quantity.intValue, unitMeasure)
      }, {
        case RelativeComparisonValue(_, operator, quantity: Int, unitMeasure) =>
          JObject(
            List(JField("value", JLong(0L)),
                 JField("operator", JString(operator)),
                 JField("quantity", JInt(quantity)),
                 JField("unitMeasure", JString(unitMeasure))))

      }))
