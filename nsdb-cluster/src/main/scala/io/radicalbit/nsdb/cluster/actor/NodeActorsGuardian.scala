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

import java.util.concurrent.TimeoutException

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorLogging, ActorRef, Deploy, OneForOneStrategy, Props, SupervisorStrategy}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.remote.RemoteScope
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.coordinator._
import io.radicalbit.nsdb.cluster.createNodeName
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetCoordinators, _}

/**
  * Actor that creates all the node singleton actors (e.g. coordinators)
  */
class NodeActorsGuardian(metadataCache: ActorRef, schemaCache: ActorRef) extends Actor with ActorLogging {

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: TimeoutException =>
      log.error(e, "Got the following TimeoutException, resuming the processing")
      Resume
    case t =>
      log.error(t, "generic error in write coordinator")
      super.supervisorStrategy.decider.apply(t)
  }

  private val selfMember = Cluster(context.system).selfMember

  private val mediator = DistributedPubSub(context.system).mediator

  val nodeName = createNodeName(selfMember)

  private val config = context.system.settings.config

  private val indexBasePath = config.getString("nsdb.index.base-path")

  private val writeToCommitLog = config.getBoolean("nsdb.commit-log.enabled")

  private val schemaCoordinator = context.actorOf(
    SchemaCoordinator
      .props(indexBasePath, schemaCache, mediator)
      .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
    s"schema-coordinator_$nodeName"
  )

  private val metadataCoordinator =
    context.actorOf(
      MetadataCoordinator.props(metadataCache, mediator).withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      name = s"metadata-coordinator_$nodeName")

  private val readCoordinator =
    context.actorOf(
      ReadCoordinator
        .props(metadataCoordinator, schemaCoordinator)
        .withDispatcher("akka.actor.control-aware-dispatcher")
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      s"read-coordinator_$nodeName"
    )
  private val publisherActor =
    context.actorOf(
      PublisherActor
        .props(readCoordinator)
        .withDispatcher("akka.actor.control-aware-dispatcher")
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      s"publisher-actor_$nodeName"
    )
  private val writeCoordinator =
    if (writeToCommitLog) {
      val commitLogger = context.actorOf(CommitLogCoordinator.props(), "commit-logger-coordinator")
      context.actorOf(
        WriteCoordinator
          .props(Some(commitLogger), metadataCoordinator, schemaCoordinator, publisherActor)
          .withDispatcher("akka.actor.control-aware-dispatcher")
          .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
        s"write-coordinator_$nodeName"
      )
    } else
      context.actorOf(
        WriteCoordinator
          .props(None, metadataCoordinator, schemaCoordinator, publisherActor)
          .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
        s"write-coordinator_$nodeName"
      )

  val metricsDataActor = context.actorOf(
    MetricsDataActor
      .props(indexBasePath)
      .withDeploy(Deploy(scope = RemoteScope(selfMember.address)))
      .withDispatcher("akka.actor.control-aware-dispatcher"),
    s"metrics-data-actor_$nodeName"
  )

  def receive: Receive = {
    case GetCoordinators =>
      sender ! CoordinatorsGot(metadataCoordinator, writeCoordinator, readCoordinator, schemaCoordinator)
    case GetPublisher        => sender() ! publisherActor
    case GetMetricsDataActor => sender() ! metricsDataActor
    case GetMetricsDataActors(replyTo) =>
      log.info("gossiping for node {}", nodeName)
      replyTo match {
        case Some(actor) => actor ! SubscribeMetricsDataActor(metricsDataActor, nodeName)
        case None        => mediator ! Publish(COORDINATORS_TOPIC, SubscribeMetricsDataActor(metricsDataActor, nodeName))
      }
  }
}

object NodeActorsGuardian {
  def props(metadataCache: ActorRef, schemaCache: ActorRef) = Props(new NodeActorsGuardian(metadataCache, schemaCache))
}
