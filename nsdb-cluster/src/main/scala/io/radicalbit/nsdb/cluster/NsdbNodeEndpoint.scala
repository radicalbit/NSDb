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

package io.radicalbit.nsdb.cluster

import akka.actor.{ActorRef, ActorSystem}
import akka.event.{Logging, LoggingAdapter}
import com.typesafe.config.Config
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.web.{BitSerializer, CustomSerializers, WebResources}
import org.json4s.{DefaultFormats, Formats}

import scala.util.Try

/**
  * Class responsible to instantiate all the node endpoints (e.g. grpc, http and ws).
  */
class NsdbNodeEndpoint(nodeId: String,
                       readCoordinator: ActorRef,
                       writeCoordinator: ActorRef,
                       metadataCoordinator: ActorRef,
                       publisher: ActorRef)(override implicit val system: ActorSystem)
    extends WebResources {

  override val config: Config = system.settings.config

  override implicit val logger: LoggingAdapter = Logging.getLogger(system, this)

  implicit val formats: Formats = DefaultFormats ++ CustomSerializers.customSerializers + BitSerializer

  /**
    * configuration key to set if security is enabled or not.
    */
  lazy val securityEnabled: Boolean = config.getBoolean("nsdb.security.enabled")

  /**
    * configuration key to set the authorization provider FQCN.
    */
  lazy val authProviderClassName: String = config.getString("nsdb.security.auth-provider-class")

  /**
    * Authorization provider instance, retrieved based on the configuration provided.
    * If there is any error during the dynamic instantiation process, a [[Left]] will be returned.
    */
  lazy val authProvider: Either[String, NSDbAuthorizationProvider] =
    if (!securityEnabled) {
      logger.info("Security is not enabled")
      Right(NSDbAuthorizationProvider.empty)
    } else if (authProviderClassName.nonEmpty) {
      logger.debug(s"Trying to load class $authProviderClassName")
      Try(Class.forName(authProviderClassName).asSubclass(classOf[NSDbAuthorizationProvider]).newInstance).toEither.left
        .map(_.getMessage)
    } else {
      Left("a valid classname must be provided if security is enabled")
    }

  authProvider match {
    case Right(provider: NSDbAuthorizationProvider) =>
      new GrpcEndpoint(
        nodeId = nodeId,
        readCoordinator = readCoordinator,
        writeCoordinator = writeCoordinator,
        metadataCoordinator = metadataCoordinator,
        authorizationProvider = provider
      )

      initWebEndpoint(nodeId, writeCoordinator, readCoordinator, metadataCoordinator, publisher, provider)
    case Left(error) =>
      logger.error(s"Error during Security initialization: $error")
      System.exit(1)
  }

}
