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
import io.radicalbit.nsdb.cluster.actor.{ClusterListener, DatabaseActorsGuardian, NodeActorsGuardian}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Success

/**
  * Creates the top level actor [[DatabaseActorsGuardian]] and grpc endpoint [[io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint]] based on coordinators
  */
trait NSDBActors {

  import akka.pattern.ask
  def system: ActorSystem

  implicit lazy val timeout: Timeout =
    Timeout(system.settings.config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit lazy val executionContext: ExecutionContextExecutor = system.dispatcher

  def initTopLevelActors() = {

    val databaseActorGuardian =
      system.actorOf(
        Props(classOf[DatabaseActorsGuardian]),
        name = "databaseActorGuardian"
      )

    Future
      .sequence(
        Seq((databaseActorGuardian ? GetMetadataCache).mapTo[ActorRef],
            (databaseActorGuardian ? GetSchemaCache).mapTo[ActorRef]))
      .onComplete {
        case Success(metadataCache :: schemaCache :: Nil) =>
          system.actorOf(
            ClusterListener.props(NodeActorsGuardian.props(metadataCache, schemaCache)),
            name = s"cluster-listener_${createNodeName(Cluster(system).selfMember)}"
          )
        case _ =>
          system.log.error("Error retrieving caches, terminating system.")
          system.terminate()
      }
  }
}

/**
  * Simply mix in [[NSDBActors]] with [[NsdbClusterConfig]]
  */
trait ProductionCluster extends NSDBActors with NsdbClusterConfig
