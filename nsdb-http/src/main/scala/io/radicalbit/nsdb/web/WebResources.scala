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
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.security.NsdbSecurity
import io.radicalbit.nsdb.ui.StaticResources
import org.json4s.DefaultFormats

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.{Failure, Success}

/**
  * Instantiate Nsdb Http Server exposing apis defined in [[ApiResources]]
  * If SSL/TLS protocol is enable in [[SSLSupport]] an Https server is started instead Http ones.
  */
trait WebResources extends StaticResources with WsResources with CorsSupport with SSLSupport { this: NsdbSecurity =>

  def config: Config

  implicit val formats = DefaultFormats

  implicit val materializer = ActorMaterializer()
  implicit val dispatcher   = system.dispatcher
  implicit val httpTimeout: Timeout =
    Timeout(config.getDuration("nsdb.http-endpoint.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
    * Nsdb cluster node guardian actor, which is the parent of node coordinators and publisher.
    *
    * @return node guardian [[ActorRef]]
    */
  def nodeGuardian: ActorRef

  def initWebEndpoint(writeCoordinator: ActorRef, readCoordinator: ActorRef, publisher: ActorRef) =
    authProvider match {
      case Success(provider) =>
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
            val port = config.getInt("akka.remote.netty.tcp.http.port")
            logger.info(s"Cluster started with http protocol on port $port")
            Http().bindAndHandle(withCors(api), config.getString("nsdb.http.interface"), port)
          }

        scala.sys.addShutdownHook {
          http.flatMap(_.unbind()).onComplete { _ =>
            system.terminate()
          }

          Await.result(system.whenTerminated, 60 seconds)
        }
      case Failure(ex) =>
        logger.error("error on loading authorization provider", ex)
        System.exit(1)
    }

}
