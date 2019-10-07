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

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.singleton._
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian.{GetMetadataCache, GetSchemaCache}
import io.radicalbit.nsdb.cluster.actor.{ClusterListener, DatabaseActorsGuardian, NodeActorsGuardian}
import io.radicalbit.nsdb.common.NsdbConfig

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

/**
  * Creates the [[ActorSystem]] based on a configuration provided by the concrete implementation
  */
trait NSDBAkkaCluster { this: NsdbConfig =>

  implicit val system: ActorSystem = ActorSystem("nsdb", config)

}

/**
  * Creates the top level actor [[DatabaseActorsGuardian]] and grpc endpoint [[GrpcEndpoint]] based on coordinators
  */
trait NSDBAActors {
  this: NSDBAkkaCluster =>

  import akka.pattern.ask

  implicit val timeout: Timeout =
    Timeout(system.settings.config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  system.actorOf(
    ClusterSingletonManager.props(singletonProps = Props(classOf[DatabaseActorsGuardian]),
                                  terminationMessage = PoisonPill,
                                  settings = ClusterSingletonManagerSettings(system)),
    name = "databaseActorGuardian"
  )

  DistributedData(system).replicator

  system.actorOf(
    ClusterSingletonProxy.props(singletonManagerPath = "/user/databaseActorGuardian",
                                settings = ClusterSingletonProxySettings(system)),
    name = "databaseActorGuardianProxy"
  )

  lazy val nodeName: String = createNodeName(Cluster(system).selfMember)

  (for {
    databaseActorGuardian <- system.actorSelection("user/databaseActorGuardianProxy").resolveOne()
    metadataCache         <- (databaseActorGuardian ? GetMetadataCache(nodeName)).mapTo[ActorRef]
    schemaCache           <- (databaseActorGuardian ? GetSchemaCache(nodeName)).mapTo[ActorRef]
  } yield {
    system.log.debug("MetadataCache and SchemaCache successfully retrieved. Creating cluster listener actor.")
    system.actorOf(
      ClusterListener.props(NodeActorsGuardian.props(metadataCache, schemaCache)),
      name = s"cluster-listener_${createNodeName(Cluster(system).selfMember)}"
    )
  }).recover {
    case e =>
      Await.result(system.terminate, 5.seconds)
      Await.result(system.whenTerminated, 5.seconds)
      system.log.error(s"Error retrieving caches $e, actor system terminated.")
  }
}

/**
  * Simply mix in [[NSDBAkkaCluster]] with [[NSDBAActors]]
  */
trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors with NsdbConfig
