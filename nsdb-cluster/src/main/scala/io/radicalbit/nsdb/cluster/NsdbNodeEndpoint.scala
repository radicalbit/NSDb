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
import akka.event.Logging
import com.typesafe.config.Config
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.security.NsdbSecurity
import io.radicalbit.nsdb.web.WebResources

/**
  * Class responsible to instantiate all the node endpoints (e.g. grpc, http and ws).
  */
class NsdbNodeEndpoint(readCoordinator: ActorRef,
                       writeCoordinator: ActorRef,
                       metadataCoordinator: ActorRef,
                       publisher: ActorRef)(override implicit val system: ActorSystem)
    extends WebResources
    with NsdbSecurity {

  override val config: Config = system.settings.config

  override protected def logger = Logging.getLogger(system, this)

  new GrpcEndpoint(readCoordinator = readCoordinator,
                   writeCoordinator = writeCoordinator,
                   metadataCoordinator = metadataCoordinator)

  initWebEndpoint(writeCoordinator, readCoordinator, publisher)

}
