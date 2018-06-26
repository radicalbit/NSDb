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

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetPublisher, GetReadCoordinator, GetWriteCoordinator}
import io.radicalbit.nsdb.security.NsdbSecurity
import io.radicalbit.nsdb.ui.StaticResources
import org.json4s.DefaultFormats

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Instantiate Nsdb Http Server exposing apis defined in [[ApiResources]]
  * If SSL/TLS protocol is enable in [[SSLSupport]] an Https server is started instead Http ones.
  */
trait Web extends StaticResources with WsResources with CorsSupport with SSLSupport { this: NsdbSecurity =>

  implicit val formats = DefaultFormats

  implicit val materializer = ActorMaterializer()
  implicit val dispatcher   = system.dispatcher
  implicit val httpTimeout: Timeout =
    Timeout(config.getDuration("nsdb.http-endpoint.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
    * Nsdb cluster guardian actor, which is the parent of top level actors.
    *
    * @return guardian actor [[ActorRef]]
    */
  def guardian: ActorRef

  authProvider match {
    case Success(provider) =>
      Future
        .sequence(
          Seq((guardian ? GetPublisher).mapTo[ActorRef],
              (guardian ? GetReadCoordinator).mapTo[ActorRef],
              (guardian ? GetWriteCoordinator).mapTo[ActorRef]))
        .onComplete {
          case Success(Seq(publisher, readCoordinator, writeCoordinator)) =>
            val api: Route = wsResources(publisher, provider) ~ new ApiResources(
              publisher,
              readCoordinator,
              writeCoordinator,
              provider).apiResources(config) ~ staticResources

            val http =
              if (isSSLEnabled) {
                val port = config.getInt("nsdb.http.https-port")
                logger.info(s"Cluster started with https protocol on port $port")
                Http().bindAndHandle(withCors(api),
                                     config.getString("nsdb.http.interface"),
                                     config.getInt("nsdb.http.https-port"),
                                     connectionContext = serverContext)
              } else {
                val port = config.getInt("nsdb.http.port")
                logger.info(s"Cluster started with http protocol on port $port")
                Http().bindAndHandle(withCors(api),
                                     config.getString("nsdb.http.interface"),
                                     config.getInt("nsdb.http.port"))
              }

            scala.sys.addShutdownHook {
              http.flatMap(_.unbind()).onComplete { _ =>
                system.terminate()
              }

              Await.result(system.whenTerminated, 60 seconds)
            }
          case Failure(ex) =>
            logger.error("error on loading coordinators", ex)
            System.exit(1)
        }
    case Failure(ex) =>
      logger.error("error on loading authorization provider", ex)
      System.exit(1)
  }

}
