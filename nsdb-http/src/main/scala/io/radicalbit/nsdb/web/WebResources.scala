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
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.monitoring.NSDbMonitoring
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import kamon.Kamon
import org.json4s.Formats

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Instantiate NSDb Http Server exposing apis defined in [[ApiResources]]
  * If SSL/TLS protocol is enable in [[SSLSupport]] an Https server is started instead Http ones.
  */
trait WebResources extends WsResources with SSLSupport {

  import CORSSupport._
  import VersionHeader._

  implicit def formats: Formats

  def config: Config

  implicit lazy val materializer = ActorMaterializer()
  implicit lazy val dispatcher   = system.dispatcher
  implicit lazy val httpTimeout: Timeout =
    Timeout(config.getDuration("nsdb.http-endpoint.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  def initWebEndpoint(nodeId: String,
                      writeCoordinator: ActorRef,
                      readCoordinator: ActorRef,
                      metadataCoordinator: ActorRef,
//<<<<<<< HEAD
                      publisher: ActorRef,
                      authProvider: NSDbAuthorizationProvider)(implicit logger: LoggingAdapter) = {
    Kamon.gauge(NSDbMonitoring.NSDbWsConnectionsTotal).withoutTags()

    val api: Route = wsResources(publisher, authProvider) ~ new ApiResources(publisher,
                                                                             readCoordinator,
                                                                             writeCoordinator,
                                                                             metadataCoordinator,
                                                                             authProvider).apiResources(config)
//=======
//                      publisher: ActorRef)(implicit logger: LoggingAdapter) =
//    authProvider match {
//      case Right(provider) =>
//        Kamon.gauge(NSDbMonitoring.NSDbWsConnectionsTotal).withoutTags()
//        val api: Route = wsResources(publisher, provider) ~ new ApiResources(publisher,
//                                                                             readCoordinator,
//                                                                             writeCoordinator,
//                                                                             metadataCoordinator,
//                                                                             provider).apiResources(config)
//>>>>>>> 4a4ef970... test grpc interceptor

    val httpExt = akka.http.scaladsl.Http()

    val http: Future[Http.ServerBinding] =
      if (isSSLEnabled) {
        val interface = config.getString(HttpInterface)
        val port      = config.getInt(HttpsPort)
        logger.info(s"Cluster Apis started for node $nodeId with https protocol at interface $interface on port $port")
        httpExt.bindAndHandle(withCors(withNSDbVersion(api)), interface, port, connectionContext = serverContext)
      } else {
        val interface = config.getString(HttpInterface)
        val port      = config.getInt(HttpPort)
        logger.info(s"Cluster Apis started for node $nodeId with http protocol at interface $interface and port $port")
        httpExt.bindAndHandle(withCors(withNSDbVersion(api)), interface, port)
      }

    scala.sys.addShutdownHook {
      http
        .flatMap(_.unbind())
        .onComplete { _ =>
          system.terminate()
        }
//<<<<<<< HEAD
      Await.result(system.whenTerminated, 60 seconds)
//=======
//      case Left(error) =>
//        logger.error(s"error on loading authorization provider $error")
//        System.exit(1)
//>>>>>>> 4a4ef970... test grpc interceptor
    }
  }

}
