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

import akka.actor.{Actor, ActorLogging, ActorRef, Deploy, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.remote.RemoteScope
import akka.util.Timeout
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.SubscribeMetricsDataActor

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

/**
  * Actor subscribed to akka cluster events. It creates all the actors needed when a node joins the cluster
  * @param writeCoordinator the global writeCoordinator
  * @param readCoordinator the global readCoordinator
  * @param metadataCoordinator the global metadataCoordinator
  */
class ClusterListener(writeCoordinator: ActorRef, readCoordinator: ActorRef, metadataCoordinator: ActorRef)
    extends Actor
    with ActorLogging {

  val cluster = Cluster(context.system)

  private val mediator = DistributedPubSub(context.system).mediator

  private val config = context.system.settings.config

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)

      val nameNode = s"${member.address.host.getOrElse("noHost")}_${member.address.port.getOrElse(2552)}"

      implicit val timeout: Timeout = Timeout(5.seconds)

      implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

      val indexBasePath = config.getString("nsdb.index.base-path")

      val metadataActor = context.system.actorOf(
        MetadataActor
          .props(indexBasePath, metadataCoordinator)
          .withDeploy(Deploy(scope = RemoteScope(member.address))),
        name = s"metadata_$nameNode"
      )

      mediator ! Subscribe("warm-up", readCoordinator)
      mediator ! Subscribe("warm-up", writeCoordinator)
      mediator ! Subscribe("metadata", metadataActor)

      log.info(s"subscribing data actor for node $nameNode")
      val metricsDataActor = context.actorOf(
        MetricsDataActor
          .props(indexBasePath)
          .withDeploy(Deploy(scope = RemoteScope(member.address)))
          .withDispatcher("akka.actor.control-aware-dispatcher"),
        s"namespace-data-actor_$nameNode"
      )
      writeCoordinator ! SubscribeMetricsDataActor(metricsDataActor, nameNode)
      readCoordinator ! SubscribeMetricsDataActor(metricsDataActor, nameNode)

    case UnreachableMember(member) =>
      log.debug("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.debug("Member is Removed: {} after {}", member.address, previousStatus)
    case _: MemberEvent => // ignore
  }
}

object ClusterListener {
  def props(writeCoordinator: ActorRef, readCoordinator: ActorRef, metadataCoordinator: ActorRef) =
    Props(new ClusterListener(writeCoordinator, readCoordinator, metadataCoordinator))
}
