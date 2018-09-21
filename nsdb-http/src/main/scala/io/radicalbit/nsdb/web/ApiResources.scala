/*
 * Copyright 2018 Radicalbit S.r.l.
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

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.web.routes._
import io.radicalbit.nsdb.web.swagger.SwaggerDocService
import org.json4s.DefaultFormats
import spray.json._

import scala.concurrent.ExecutionContext

object Formats extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object JSerializableJsonFormat extends RootJsonFormat[JSerializable] {
    def write(c: JSerializable) = c match {
      case v: java.lang.Double  => JsNumber(v)
      case v: java.lang.Long    => JsNumber(v)
      case v: java.lang.Integer => JsNumber(v)
      case v                    => JsString(v.toString)
    }
    def read(value: JsValue) = value match {
      case JsNumber(v) if v.scale > 0   => new java.lang.Double(v.doubleValue)
      case JsNumber(v) if v.isValidLong => new java.lang.Long(v.longValue)
      case JsNumber(v) if v.isValidInt  => new java.lang.Integer(v.intValue)
      case JsString(v)                  => v
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

  implicit val FilterOperatorFormat: RootJsonFormat[FilterOperators.Value]  = enumFormat(FilterOperators)
  implicit val CheckOperatorFormat: RootJsonFormat[NullableOperators.Value] = enumFormat(NullableOperators)
  implicit val FilterByValueFormat                                          = jsonFormat3(FilterByValue.apply)
  implicit val FilterNullableValueFormat                                    = jsonFormat2(FilterNullableValue.apply)

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

  implicit val QbFormat = jsonFormat7(QueryBody.apply)

  implicit val BitFormat = jsonFormat4(Bit.apply)

  implicit val InsertBodyFormat = jsonFormat4(InsertBody.apply)

}

class ApiResources(val publisherActor: ActorRef,
                   val readCoordinator: ActorRef,
                   val writeCoordinator: ActorRef,
                   val authenticationProvider: NSDBAuthProvider)(implicit val timeout: Timeout, logger: LoggingAdapter)
    extends CommandApi
    with QueryApi
    with DataApi {

  implicit val formats: DefaultFormats = DefaultFormats

  def healthCheckApi: Route = {
    pathPrefix("status") {
      (pathEnd & get) {
        complete("RUNNING")
      }
    }
  }

  def swagger =
    path("swagger") { getFromResource("swagger-ui/index.html") } ~
      getFromResourceDirectory("swagger-ui")

  def apiResources(config: Config)(implicit ec: ExecutionContext): Route =
    queryApi ~
      dataApi ~
      healthCheckApi ~
      commandsApi ~
      swagger ~
      new SwaggerDocService().routes

}
