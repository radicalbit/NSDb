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
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor._
import io.radicalbit.nsdb.common.NsdbConfig

import scala.concurrent.ExecutionContextExecutor

/**
  * Creates the [[ActorSystem]] based on a configuration provided by the concrete implementation
  */
trait NSDBAkkaCluster { this: NsdbConfig with NSDBActors =>

  implicit val system: ActorSystem = ActorSystem("nsdb", config)

  initTopLevelActors()
}

/**
  * Creates the top level actor [[DatabaseActorsGuardian]] and grpc endpoint [[GrpcEndpoint]] based on coordinators
  */
trait NSDBActors {

  implicit def system: ActorSystem

  implicit lazy val timeout: Timeout =
    Timeout(system.settings.config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit lazy val executionContext: ExecutionContextExecutor = system.dispatcher

  def initTopLevelActors(): Unit = {
    system.actorOf(
      ClusterSingletonManager.props(singletonProps = Props(classOf[DatabaseActorsGuardian]),
                                    terminationMessage = PoisonPill,
                                    settings = ClusterSingletonManagerSettings(system)),
      name = "databaseActorGuardian"
    )

    AkkaManagement(system).start()
    ClusterBootstrap(system).start()

    DistributedData(system).replicator

    system.actorOf(
      ClusterSingletonProxy.props(singletonManagerPath = "/user/databaseActorGuardian",
                                  settings = ClusterSingletonProxySettings(system)),
      name = "databaseActorGuardianProxy"
    )

    lazy val nodeName: String = createNodeName(Cluster(system).selfMember)

    system.actorOf(
      ClusterListener.props(
        NodeActorsGuardian.props(
          system.actorOf(Props[ReplicatedMetadataCache], s"metadata-cache-$nodeName"),
          system.actorOf(Props[ReplicatedSchemaCache], s"schema-cache-$nodeName")
        )),
      name = s"cluster-listener_${createNodeName(Cluster(system).selfMember)}"
    )
  }
}

/**
  * Simply mix in [[NSDBAkkaCluster]] with [[NSDBActors]]
  */
trait ProductionCluster extends NSDBAkkaCluster with NSDBActors with NsdbClusterConfig
