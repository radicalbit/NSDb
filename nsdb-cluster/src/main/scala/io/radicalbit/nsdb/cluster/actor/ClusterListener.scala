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
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.{NsdbNodeEndpoint, createNodeName}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

/**
  * Actor subscribed to akka cluster events. It creates all the actors needed when a node joins the cluster
  * @param nodeActorsGuardianProps props of NodeActorGuardian actor.
  */
class ClusterListener(nodeActorsGuardianProps: Props) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  private val mediator = DistributedPubSub(context.system).mediator

  private val config = context.system.settings.config

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    log.info("Created ClusterListener at path {} and subscribed to member events", self.path)
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member)
        if member.address.host.isDefined &&
          member.address.host.get == config.getString("akka.remote.netty.tcp.hostname") &&
          member.address.port.isDefined &&
          member.address.port.get == config.getInt("akka.remote.netty.tcp.port") =>
      log.info("Member is Up: {}", member.address)

      val nodeName = createNodeName(member)

      implicit val timeout: Timeout = Timeout(5.seconds)

      val indexBasePath = config.getString("nsdb.index.base-path")

      val nodeActorsGuardian =
        context.system.actorOf(nodeActorsGuardianProps.withDeploy(Deploy(scope = RemoteScope(member.address))),
                               name = s"guardian_$nodeName")

      (nodeActorsGuardian ? GetNodeChildActors)
        .map {
          case NodeChildActorsGot(metadataCoordinator,
                                  writeCoordinator,
                                  readCoordinator,
                                  schemaCoordinator,
                                  publisherActor: ActorRef) =>
//            mediator ! Subscribe(WARMUP_TOPIC, readCoordinator)
//            mediator ! Subscribe(WARMUP_TOPIC, writeCoordinator)

//            val metadataActor = context.system.actorOf(MetadataActor
//                                                         .props(indexBasePath, metadataCoordinator)
//                                                         .withDeploy(Deploy(scope = RemoteScope(member.address))),
//                                                       name = s"metadata_$nodeName")
//            val schemaActor = context.system.actorOf(SchemaActor
//                                                       .props(indexBasePath, schemaCoordinator)
//                                                       .withDeploy(Deploy(scope = RemoteScope(member.address))),
//                                                     name = s"schema-actor_$nodeName")
//
//            mediator ! Subscribe(METADATA_TOPIC, metadataActor)
//            mediator ! Subscribe(SCHEMA_TOPIC, schemaActor)

            mediator ! Subscribe(NODE_GUARDIANS_TOPIC, nodeActorsGuardian)

            new NsdbNodeEndpoint(readCoordinator, writeCoordinator, metadataCoordinator, publisherActor)(context.system)
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

  def props(nodeActorsGuardianProps: Props) =
    Props(new ClusterListener(nodeActorsGuardianProps))
}
