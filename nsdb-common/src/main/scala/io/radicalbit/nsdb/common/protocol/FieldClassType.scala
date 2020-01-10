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

package io.radicalbit.nsdb.common.protocol

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

/**
  * Algebraic Data Type representing the field class types provided by NSDb.
  */

@JsonSerialize(using = classOf[FieldClassTypeJsonSerializer])
@JsonDeserialize(using = classOf[FieldClassTypeJsonDeserializer])
sealed trait FieldClassType

case object TimestampFieldType extends FieldClassType
case object ValueFieldType     extends FieldClassType
case object DimensionFieldType extends FieldClassType
case object TagFieldType       extends FieldClassType

class FieldClassTypeJsonSerializer extends StdSerializer[FieldClassType](classOf[FieldClassType]) {

  override def serialize(value: FieldClassType, gen: JsonGenerator, provider: SerializerProvider): Unit =
//    val strValue = value match {
//      case TimestampFieldType => "TimestampFieldType"
//      case ValueFieldType     => "ValueFieldType"
//      case DimensionFieldType => "DimensionFieldType"
//      case TagFieldType       => "TagFieldType"
//    }
    gen.writeString(value.toString)

}

class FieldClassTypeJsonDeserializer extends StdDeserializer[FieldClassType](classOf[FieldClassType]) {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): FieldClassType = {
    p.getText match {
      case "TimestampFieldType" => TimestampFieldType
      case "ValueFieldType"     => ValueFieldType
      case "DimensionFieldType" => DimensionFieldType
      case "TagFieldType"       => TagFieldType
    }
  }
}
