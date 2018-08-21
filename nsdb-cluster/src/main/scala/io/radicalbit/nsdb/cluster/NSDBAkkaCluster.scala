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

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian.{GetMetadataCache, GetSchemaCache}
import io.radicalbit.nsdb.cluster.actor.{ClusterListener, DatabaseActorsGuardian}
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.common.NsdbConfig

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Success

/**
  * Creates the [[ActorSystem]] based on a configuration provided by the concrete implementation
  */
trait NSDBAkkaCluster { this: NsdbConfig =>

  implicit val system: ActorSystem = ActorSystem("nsdb", config)

}

/**
  * Creates the top level actor [[DatabaseActorsGuardian]] and grpc endpoint [[GrpcEndpoint]] based on coordinators
  */
trait NSDBAActors { this: NSDBAkkaCluster =>

  import akka.pattern.ask

  implicit val timeout: Timeout =
    Timeout(system.settings.config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val databaseActorGuardian = system.actorOf(
    Props(classOf[DatabaseActorsGuardian]),
    name = "databaseActorGuardian"
  )

  Future
    .sequence(
      Seq((databaseActorGuardian ? GetMetadataCache).mapTo[ActorRef],
          (databaseActorGuardian ? GetSchemaCache).mapTo[ActorRef]))
    .onComplete {
      case Success(m :: s :: Nil) =>
        system.actorOf(
          ClusterListener.props(m, s),
          name = s"cluster-listener_${createNodeName(Cluster(system).selfMember)}"
        )
      case _ =>
        system.log.error("Error retrieving caches, terminating system.")
        system.terminate()
    }
}

/**
  * Simply mix in [[NSDBAkkaCluster]] with [[NSDBAActors]]
  */
trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors with NsdbConfig
