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

package io.radicalbit.nsdb.cluster

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{CoordinatorsGot, GetCoordinators, GetPublisher}
import io.radicalbit.nsdb.security.NsdbSecurity
import io.radicalbit.nsdb.web.WebResources

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Class responsible to instantiate all the node endpoints (e.g. grpc, http and ws).
  * @param nodeGuardian the node guardian actor, which is the parent of the node coordinators and publisher.
  * @param system the global ActorSystem.
  */
class NsdbNodeEndpoint(override val nodeGuardian: ActorRef)(override implicit val system: ActorSystem)
    extends WebResources
    with NsdbSecurity
    with NsdbConfig
    with LazyLogging {

  Future
    .sequence(
      Seq((nodeGuardian ? GetCoordinators).mapTo[CoordinatorsGot], (nodeGuardian ? GetPublisher).mapTo[ActorRef]))
    .onComplete {

      case Success(
          Seq(CoordinatorsGot(metadataCoordinator,
                              writeCoordinator: ActorRef,
                              readCoordinator: ActorRef,
                              schemaCoordinator: ActorRef),
              publisher: ActorRef)) =>
        new GrpcEndpoint(readCoordinator = readCoordinator,
                         writeCoordinator = writeCoordinator,
                         metadataCoordinator = metadataCoordinator)

        initWebEndpoint(writeCoordinator, readCoordinator, publisher)

      case Failure(ex) =>
        logger.error("error on loading coordinators", ex)
        System.exit(1)
    }

}
