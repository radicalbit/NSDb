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

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.web.routes._
import io.radicalbit.nsdb.web.swagger.SwaggerDocService
import org.json4s.Formats

import scala.concurrent.ExecutionContext

class ApiResources(val publisherActor: ActorRef,
                   val readCoordinator: ActorRef,
                   val writeCoordinator: ActorRef,
                   val metadataCoordinator: ActorRef,
                   val authorizationProvider: NSDbAuthorizationProvider)(override implicit val timeout: Timeout,
                                                                         override implicit val logger: LoggingAdapter,
                                                                         override implicit val ec: ExecutionContext,
                                                                         override implicit val formats: Formats)
    extends CommandApi
    with QueryApi
    with QueryValidationApi
    with DataApi {

  def healthCheckApi: Route = {
    pathPrefix("status") {
      (pathEnd & get) {
        complete("RUNNING")
      }
    }
  }

  def swagger: Route =
    path("swagger") { getFromResource("swagger-ui/index.html") } ~
      getFromResourceDirectory("swagger-ui")

  def apiResources(config: Config)(implicit ec: ExecutionContext): Route =
    queryApi ~
      queryValidationApi ~
      dataApi ~
      healthCheckApi ~
      commandsApi ~
      swagger ~
      new SwaggerDocService(config.getString(HttpInterface), config.getInt(HttpPort)).routes
}
