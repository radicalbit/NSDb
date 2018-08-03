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

package io.radicalbit.nsdb.cluster.actor

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.{NsdbNodeEndpoint, createNodeName}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{CoordinatorsGot, GetCoordinators, GetMetricsDataActors}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

/**
  * Actor subscribed to akka cluster events. It creates all the actors needed when a node joins the cluster
  * @param metadataCache the global metadata cache actor.
  * @param schemaCache the global schema cache actor.
  */
class ClusterListener(metadataCache: ActorRef, schemaCache: ActorRef) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  private val mediator = DistributedPubSub(context.system).mediator

  private val config = context.system.settings.config

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    log.info("Created ClusterListener at path {} and subscribed to member events", self.path)
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member)
        if member.address.port.isDefined && member.address.port.get == config.getInt("akka.remote.netty.tcp.port") =>
      log.info("Member is Up: {}", member.address)

      val nodeName = createNodeName(member)

      implicit val timeout: Timeout = Timeout(5.seconds)

      implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

      val indexBasePath = config.getString("nsdb.index.base-path")

      val nodeActorsGuardian =
        context.system.actorOf(NodeActorsGuardian.props(metadataCache, schemaCache), name = s"guardian_$nodeName")

      mediator ! Subscribe(NODE_GUARDIANS_TOPIC, nodeActorsGuardian)

      (nodeActorsGuardian ? GetCoordinators)
        .map {
          case CoordinatorsGot(metadataCoordinator, writeCoordinator, readCoordinator, schemaCoordinator) =>
            val metadataActor = context.system.actorOf(MetadataActor
                                                         .props(indexBasePath, metadataCoordinator)
                                                         .withDeploy(Deploy(scope = RemoteScope(member.address))),
                                                       name = s"metadata_$nodeName")
            val schemaActor = context.system.actorOf(SchemaActor
                                                       .props(indexBasePath, schemaCoordinator)
                                                       .withDeploy(Deploy(scope = RemoteScope(member.address))),
                                                     name = s"schema-actor_$nodeName")

            mediator ! Subscribe(WARMUP_TOPIC, readCoordinator)
            mediator ! Subscribe(WARMUP_TOPIC, writeCoordinator)

            mediator ! Subscribe(METADATA_TOPIC, metadataActor)
            mediator ! Subscribe(SCHEMA_TOPIC, schemaActor)

            for {
              _ <- mediator ? Subscribe(COORDINATORS_TOPIC, writeCoordinator)
              _ <- mediator ? Subscribe(COORDINATORS_TOPIC, readCoordinator)
            } yield {
              log.info("requesting metrics data actors after node {} joined", nodeName)
              mediator ! Publish(NODE_GUARDIANS_TOPIC, GetMetricsDataActors())
            }

            new NsdbNodeEndpoint(nodeActorsGuardian)(context.system)

          case _ =>
        }
    case UnreachableMember(member) =>
      log.debug("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.debug("Member is Removed: {} after {}", member.address, previousStatus)
    case _: MemberEvent => // ignore
  }
}

object ClusterListener {
  def props(metadataCache: ActorRef, schemaCache: ActorRef) =
    Props(new ClusterListener(metadataCache, schemaCache))
}
