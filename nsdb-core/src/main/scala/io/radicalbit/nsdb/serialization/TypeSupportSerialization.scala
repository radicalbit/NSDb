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

package io.radicalbit.nsdb.serialization

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import io.radicalbit.nsdb.index._

object TypeSupportSerialization {

//  class IndexTypeJsonSerializer extends StdSerializer[IndexType[_]](classOf[IndexType[_]]) {
//
//    override def serialize(value: IndexType[_], gen: JsonGenerator, provider: SerializerProvider): Unit =
//      gen.writeString(value.toString)
//  }
//
//  class IndexTypeJsonDeserializer extends StdDeserializer[IndexType[_]](classOf[IndexType[_]]) {
//
//    override def deserialize(p: JsonParser, ctxt: DeserializationContext): IndexType[_] = {
//      p.getText match {
//        case "BIGINT"  => BIGINT
//        case "DECIMAL" => DECIMAL
//        case "INT"     => INT
//        case "VARCHAR" => VARCHAR
//      }
//    }
//  }

}
