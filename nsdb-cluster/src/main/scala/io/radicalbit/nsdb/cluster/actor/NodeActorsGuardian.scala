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

import java.util.concurrent.{TimeUnit, TimeoutException}
import akka.actor.SupervisorStrategy.{Resume, Stop}
import akka.actor.{ActorContext, ActorRef, _}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.remote.RemoteScope
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.supervision.OneForOneWithRetriesStrategy
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.coordinator._
import io.radicalbit.nsdb.cluster.createNodeName
import io.radicalbit.nsdb.cluster.logic.{CapacityWriteNodesSelectionLogic, LocalityReadNodesSelection}
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.exception.InvalidLocationsInNode
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.util.FileUtils

import scala.concurrent.duration.FiniteDuration

/**
  * Actor that creates all the node singleton actors (e.g. coordinators)
  */
class NodeActorsGuardian extends Actor with ActorLogging {

  private val selfMember = Cluster(context.system).selfMember

  private val mediator = DistributedPubSub(context.system).mediator

  private val nodeAddress = createNodeName(selfMember)

  protected lazy val selfNodeName: String = createNodeName(selfMember)
  protected lazy val nodeId: String       = FileUtils.getOrCreateNodeId(selfNodeName, config.getString(NSDBMetadataPath))

  private val config = context.system.settings.config

  private val indexBasePath = config.getString(StorageIndexPath)

  if (config.hasPath(StorageTmpPath))
    System.setProperty("java.io.tmpdir", config.getString(StorageTmpPath))

  protected val actorNameSuffix = s"${nodeAddress}_$nodeId"

  private lazy val writeNodesSelectionLogic = new CapacityWriteNodesSelectionLogic(
    CapacityWriteNodesSelectionLogic.fromConfigValue(config.getString("nsdb.cluster.metrics-selector")))
  private lazy val readNodesSelection = new LocalityReadNodesSelection(nodeId)

  private lazy val maxAttempts = context.system.settings.config.getInt("nsdb.write.retry-attempts")

  private val metadataCache = context.actorOf(Props[ReplicatedMetadataCache], s"metadata-cache-$nodeId-$nodeAddress")
  private val schemaCache   = context.actorOf(Props[ReplicatedSchemaCache], s"schema-cache-$nodeId-$nodeAddress")

  def shutdownBehaviour(context: ActorContext, child: ActorRef): Unit =
    context.system.terminate()

  override val supervisorStrategy: SupervisorStrategy = OneForOneWithRetriesStrategy(maxNrOfRetries = maxAttempts) {
    case e: TimeoutException =>
      log.error(e, "Got the following TimeoutException, resuming the processing")
      Resume
    case e: InvalidLocationsInNode =>
      log.error(e, s"Invalid locations ${e.locations} found")
      context.system.terminate()
      Stop
    case t =>
      log.error(t, "generic error occurred")
      Resume
  } {
    shutdownBehaviour
  }

  def createClusterListener: ActorRef =
    context.actorOf(Props[ClusterListener], name = s"cluster-listener_${createNodeName(selfMember)}")

  private val clusterListener: ActorRef = createClusterListener

  protected lazy val schemaCoordinator: ActorRef = context.actorOf(
    SchemaCoordinator
      .props(schemaCache)
      .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
    s"schema-coordinator_$actorNameSuffix"
  )

  protected lazy val metadataCoordinator: ActorRef =
    context.actorOf(
      MetadataCoordinator
        .props(clusterListener, metadataCache, schemaCache, mediator, writeNodesSelectionLogic)
        .withDispatcher("akka.actor.control-aware-dispatcher")
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      name = s"metadata-coordinator_$actorNameSuffix"
    )

  protected lazy val readCoordinator: ActorRef =
    context.actorOf(
      ReadCoordinator
        .props(metadataCoordinator, schemaCoordinator, mediator, readNodesSelection)
        .withDispatcher("akka.actor.control-aware-dispatcher")
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      s"read-coordinator_$actorNameSuffix"
    )
  protected lazy val publisherActor: ActorRef =
    context.actorOf(
      PublisherActor
        .props(readCoordinator)
        .withDispatcher("akka.actor.control-aware-dispatcher")
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      s"publisher-actor_$actorNameSuffix"
    )
  protected lazy val writeCoordinator: ActorRef =
    context.actorOf(
      WriteCoordinator
        .props(metadataCoordinator, schemaCoordinator, mediator)
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      s"write-coordinator_$actorNameSuffix"
    )

  protected lazy val commitLogCoordinator: ActorRef =
    context.actorOf(
      Props[CommitLogCoordinator]
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address)))
        .withDispatcher("akka.actor.control-aware-dispatcher"),
      s"commitlog-coordinator_$actorNameSuffix"
    )

  protected lazy val metricsDataActor: ActorRef = context.actorOf(
    MetricsDataActor
      .props(indexBasePath, nodeAddress, commitLogCoordinator)
      .withDeploy(Deploy(scope = RemoteScope(selfMember.address)))
      .withDispatcher("akka.actor.control-aware-dispatcher"),
    s"metrics-data-actor_$actorNameSuffix"
  )

  override def preStart(): Unit = {
    import context.dispatcher

    val interval = FiniteDuration(
      context.system.settings.config.getDuration("nsdb.heartbeat.interval", TimeUnit.SECONDS),
      TimeUnit.SECONDS)

    /**
      * scheduler that disseminate gossip message to all the cluster listener actors
      */
    context.system.scheduler.schedule(interval, interval) {
      mediator ! Publish(NSDB_LISTENERS_TOPIC, NodeAlive(nodeId, nodeAddress))
    }

    log.info(s"NodeActorGuardian is ready at ${self.path.name}")
  }

  def receive: Receive = {
    case GetNodeChildActors =>
      sender ! NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, publisherActor)
    case GetMetricsDataActors =>
      log.debug("gossiping metric data actors from node {}", nodeId)
      mediator ! Publish(COORDINATORS_TOPIC, SubscribeMetricsDataActor(metricsDataActor, nodeId))
    case GetCommitLogCoordinators =>
      log.debug("gossiping commit logs for node {}", nodeId)
      mediator ! Publish(COORDINATORS_TOPIC, SubscribeCommitLogCoordinator(commitLogCoordinator, nodeId))
    case GetPublishers =>
      log.debug("gossiping publishers for node {}", nodeId)
      mediator ! Publish(COORDINATORS_TOPIC, SubscribePublisher(publisherActor, nodeId))
  }
}
