/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import akka.actor.SupervisorStrategy.{Resume, Stop}
import akka.actor._
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.remote.RemoteScope
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.coordinator._
import io.radicalbit.nsdb.cluster.createNodeName
import io.radicalbit.nsdb.cluster.logic.{CapacityWriteNodesSelectionLogic, LocalityReadNodesSelection}
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.common.exception.TooManyRetriesException
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._

/**
  * Actor that creates all the node singleton actors (e.g. coordinators)
  */
class NodeActorsGuardian(clusterListener: ActorRef) extends Actor with ActorLogging {

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: TimeoutException =>
      log.error(e, "Got the following TimeoutException, resuming the processing")
      Resume
    case e: TooManyRetriesException =>
      log.error(e, "Too many retries on the node")
      context.system.terminate()
      Stop
    case t =>
      log.error(t, "generic error occurred")
      super.supervisorStrategy.decider.apply(t)
  }

  private val selfMember = Cluster(context.system).selfMember

  private val mediator = DistributedPubSub(context.system).mediator

  private val nodeName = createNodeName(selfMember)

  private val config = context.system.settings.config

  private val indexBasePath = config.getString(StorageIndexPath)

  if (config.hasPath(StorageTmpPath))
    System.setProperty("java.io.tmpdir", config.getString(StorageTmpPath))

  private lazy val writeNodesSelectionLogic = new CapacityWriteNodesSelectionLogic(
    CapacityWriteNodesSelectionLogic.fromConfigValue(config.getString("nsdb.cluster.metrics-selector")))
  private lazy val readNodesSelection = new LocalityReadNodesSelection(nodeName)

  private val metadataCache = context.actorOf(Props[ReplicatedMetadataCache], s"metadata-cache-$nodeName")
  private val schemaCache   = context.actorOf(Props[ReplicatedSchemaCache], s"schema-cache-$nodeName")

  private val schemaCoordinator = context.actorOf(
    SchemaCoordinator
      .props(schemaCache)
      .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
    s"schema-coordinator_$nodeName"
  )

  private val metadataCoordinator =
    context.actorOf(
      MetadataCoordinator
        .props(clusterListener, metadataCache, schemaCache, mediator, writeNodesSelectionLogic)
        .withDispatcher("akka.actor.control-aware-dispatcher")
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      name = s"metadata-coordinator_$nodeName"
    )

  private val readCoordinator =
    context.actorOf(
      ReadCoordinator
        .props(metadataCoordinator, schemaCoordinator, mediator, readNodesSelection)
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
    context.actorOf(
      WriteCoordinator
        .props(metadataCoordinator, schemaCoordinator, mediator)
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      s"write-coordinator_$nodeName"
    )

  private val commitLogCoordinator =
    context.actorOf(
      Props[CommitLogCoordinator]
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address)))
        .withDispatcher("akka.actor.control-aware-dispatcher"),
      s"commitlog-coordinator_$nodeName"
    )

  private val metricsDataActor = context.actorOf(
    MetricsDataActor
      .props(indexBasePath, nodeName, commitLogCoordinator)
      .withDeploy(Deploy(scope = RemoteScope(selfMember.address)))
      .withDispatcher("akka.actor.control-aware-dispatcher"),
    s"metrics-data-actor_$nodeName"
  )

  def receive: Receive = {
    case GetNodeChildActors =>
      sender ! NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, publisherActor)
    case GetMetricsDataActors =>
      log.debug("gossiping metric data actors from node {}", nodeName)
      mediator ! Publish(COORDINATORS_TOPIC, SubscribeMetricsDataActor(metricsDataActor, nodeName))
    case GetCommitLogCoordinators =>
      log.debug("gossiping commit logs for node {}", nodeName)
      mediator ! Publish(COORDINATORS_TOPIC, SubscribeCommitLogCoordinator(commitLogCoordinator, nodeName))
    case GetPublishers =>
      log.debug("gossiping publishers for node {}", nodeName)
      mediator ! Publish(COORDINATORS_TOPIC, SubscribePublisher(publisherActor, nodeName))
  }
}

object NodeActorsGuardian {
  def props(clusterListener: ActorRef): Props =
    Props(new NodeActorsGuardian(clusterListener))
}
