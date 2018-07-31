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
import akka.cluster.singleton._
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian.{GetMetadataCache, GetSchemaCache}
import io.radicalbit.nsdb.cluster.actor.{ClusterListener, DatabaseActorsGuardian}
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.common.NsdbConfig

import scala.concurrent.Future
import scala.util.Success

/**
  * Creates the [[ActorSystem]] based on a configuration provided by the concrete implementation
  */
trait NSDBAkkaCluster { this: NsdbConfig =>

  implicit lazy val system: ActorSystem = ActorSystem("nsdb", config)
}

/**
  * Creates the top level actor [[DatabaseActorsGuardian]] and grpc endpoint [[GrpcEndpoint]] based on coordinators
  */
trait NSDBAActors { this: NSDBAkkaCluster =>

  import akka.pattern.ask

  implicit val timeout =
    Timeout(system.settings.config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val executionContext = system.dispatcher

  //FIXME mock message
  case object End

  system.actorOf(
    ClusterSingletonManager.props(singletonProps = Props(classOf[DatabaseActorsGuardian]),
                                  terminationMessage = End,
                                  settings = ClusterSingletonManagerSettings(system)),
    name = "databaseActorGuardian"
  )

  val databaseActorGuardianProxy = system.actorOf(
    ClusterSingletonProxy.props(singletonManagerPath = "/user/databaseActorGuardian",
                                settings = ClusterSingletonProxySettings(system)),
    name = "databaseActorGuardianProxy"
  )

  lazy val metadataCache = (databaseActorGuardianProxy ? GetMetadataCache).mapTo[ActorRef]
  lazy val schemaCache   = (databaseActorGuardianProxy ? GetSchemaCache).mapTo[ActorRef]

  Future.sequence(Seq(metadataCache, schemaCache)).onComplete {
    case Success(m :: s :: Nil) =>
      system.actorOf(
        ClusterListener.props(m, s),
        name = "clusterListener"
      )
    case e =>
      system.log.error("Error retrieving caches, terminating system.")
      system.terminate()
  }

}

/**
  * Simply mix in [[NSDBAkkaCluster]] with [[NSDBAActors]]
  */
trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors with NsdbConfig
