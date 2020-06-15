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

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import io.radicalbit.nsdb.actors.RealTimeProtocol.Events._
import io.radicalbit.nsdb.actors.RealTimeProtocol.RealTimeOutGoingMessage
import io.radicalbit.nsdb.common._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.web.actor.StreamActor.RegisterQuery
import io.radicalbit.nsdb.web.routes._
import io.radicalbit.nsdb.web.validation.FieldErrorInfo
import spray.json._

object NSDbJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object NSDbTypeJsonFormat extends RootJsonFormat[NSDbType] {
    def write(c: NSDbType): JsValue = c match {
      case NSDbDoubleType(v) => JsNumber(v)
      case NSDbLongType(v)   => JsNumber(v)
      case NSDbIntType(v)    => JsNumber(v)
      case NSDbStringType(v) => JsString(v)
    }
    def read(value: JsValue): NSDbType = value match {
      case JsNumber(v) if v.scale > 0   => NSDbDoubleType(v.doubleValue)
      case JsNumber(v) if v.isValidLong => NSDbLongType(v.longValue)
      case JsNumber(v) if v.isValidInt  => NSDbIntType(v.intValue)
      case JsString(v)                  => NSDbStringType(v)
    }
  }

  implicit object NSDbNumericTypeJsonFormat extends RootJsonFormat[NSDbNumericType] {
    def write(c: NSDbNumericType): JsValue = c match {
      case NSDbDoubleType(v) => JsNumber(v)
      case NSDbLongType(v)   => JsNumber(v)
      case NSDbIntType(v)    => JsNumber(v)
    }
    def read(value: JsValue): NSDbNumericType = value match {
      case JsNumber(v) if v.scale > 0   => NSDbDoubleType(v.doubleValue)
      case JsNumber(v) if v.isValidLong => NSDbLongType(v.longValue)
      case JsNumber(v) if v.isValidInt  => NSDbIntType(v.intValue)
    }
  }

  implicit object NSDbLongTypeJsonFormat extends RootJsonFormat[NSDbLongType] {
    def write(c: NSDbLongType): JsValue = JsNumber(c.rawValue)
    def read(value: JsValue): NSDbLongType = value match {
      case JsNumber(v) if v.isValidLong => NSDbLongType(v.longValue)
      case JsNumber(v) if v.isValidInt  => NSDbLongType(v.intValue)
      case json                         => deserializationError(s"cannot deserialize a long from $json")
    }
  }

  implicit def enumFormat[T <: Enumeration](implicit enu: T): RootJsonFormat[T#Value] =
    new RootJsonFormat[T#Value] {
      def write(obj: T#Value): JsValue = JsString(obj.toString)
      def read(json: JsValue): T#Value = {
        json match {
          case JsString(txt) => enu.withName(txt.replace(" ", "").toUpperCase)
          case somethingElse =>
            throw DeserializationException(s"Expected a value from enum $enu instead of $somethingElse")
        }
      }
    }

  implicit val FilterOperatorFormat: RootJsonFormat[FilterOperators.Value]    = enumFormat(FilterOperators)
  implicit val CheckOperatorFormat: RootJsonFormat[NullableOperators.Value]   = enumFormat(NullableOperators)
  implicit val FilterByValueFormat: RootJsonFormat[FilterByValue]             = jsonFormat3(FilterByValue.apply)
  implicit val FilterNullableValueFormat: RootJsonFormat[FilterNullableValue] = jsonFormat2(FilterNullableValue.apply)

  implicit object FilterJsonFormat extends RootJsonFormat[Filter] {
    def write(a: Filter) = a match {
      case f: FilterByValue       => f.toJson
      case f: FilterNullableValue => f.toJson
    }
    def read(value: JsValue): Filter =
      value.asJsObject.fields.get("value") match {
        case Some(_) => value.convertTo[FilterByValue]
        case None    => value.convertTo[FilterNullableValue]
      }
  }

  implicit val QbRegisterQueryFormat: RootJsonFormat[RegisterQuery] = jsonFormat7(RegisterQuery.apply)

  implicit val QbFormat: RootJsonFormat[QueryBody] = jsonFormat8(QueryBody.apply)

  implicit val QvbFormat: RootJsonFormat[QueryValidationBody] = jsonFormat4(QueryValidationBody.apply)

  implicit val BitFormat: RootJsonFormat[Bit] = jsonFormat4(Bit.apply)

  implicit val InsertBodyFormat: RootJsonFormat[InsertBody] = jsonFormat4(InsertBody.apply)

  implicit val FieldErrorInfoFormat: RootJsonFormat[FieldErrorInfo] = jsonFormat2(FieldErrorInfo.apply)

  implicit object RealTimeOutGoingMessageWriter extends JsonWriter[RealTimeOutGoingMessage] {

    implicit val subscribedByQueryStringFormat: RootJsonFormat[SubscribedByQueryString] = jsonFormat5(
      SubscribedByQueryString.apply)
    implicit val SubscriptionByQueryStringFailedFormat: RootJsonFormat[SubscriptionByQueryStringFailed] = jsonFormat5(
      SubscriptionByQueryStringFailed.apply)
    implicit val recordsPublishedFormat: RootJsonFormat[RecordsPublished] = jsonFormat3(RecordsPublished.apply)
    implicit val errorResponseFormat: RootJsonFormat[ErrorResponse]       = jsonFormat1(ErrorResponse.apply)

    def write(a: RealTimeOutGoingMessage): JsValue = a match {
      case msg: SubscribedByQueryString         => msg.toJson
      case msg: SubscriptionByQueryStringFailed => msg.toJson
      case msg: RecordsPublished                => msg.toJson
      case msg: ErrorResponse                   => msg.toJson
    }
  }
}
