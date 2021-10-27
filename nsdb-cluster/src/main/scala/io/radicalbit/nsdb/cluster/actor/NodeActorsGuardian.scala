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

import akka.actor.SupervisorStrategy.{Resume, Stop}
import akka.actor.{ActorContext, ActorRef, _}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.supervision.OneForOneWithRetriesStrategy
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.actor.NodeActorsGuardian.{GetNode, NodeGot, UpdateVolatileId, VolatileIdUpdated}
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache.{
  GetNodesBlackListFromCache,
  GetNodesBlackListFromCacheFailed,
  GetNodesBlackListFromCacheResponse,
  NodesBlackListFromCacheGot
}
import io.radicalbit.nsdb.cluster.coordinator._
import io.radicalbit.nsdb.cluster.createNodeAddress
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.cluster.logic.{CapacityWriteNodesSelectionLogic, LocalityReadNodesSelection}
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.common.protocol.{NSDbNode, NSDbSerializable}
import io.radicalbit.nsdb.exception.InvalidLocationsInNode
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.util.FileUtils

import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  * Actor that creates all the node singleton actors (e.g. coordinators)
  */
class NodeActorsGuardian extends Actor with ActorLogging {

  private val selfMember = Cluster(context.system).selfMember

  protected val mediator = DistributedPubSub(context.system).mediator

  protected val nodeAddress: String = createNodeAddress(selfMember)

  protected lazy val selfNodeName: String = createNodeAddress(selfMember)
  protected lazy val nodeFsId: String     = FileUtils.getOrCreateNodeFsId(selfNodeName, config.getString(NSDBMetadataPath))

  private val config = context.system.settings.config

  private val indexBasePath = config.getString(StorageIndexPath)

  protected val heartbeatInterval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.heartbeat.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  implicit val defaultTimeout: Timeout =
    Timeout(config.getDuration(globalTimeout, TimeUnit.SECONDS), TimeUnit.SECONDS)

  protected var node: NSDbNode                   = _
  protected var heartBeatDispatcher: Cancellable = _

  if (config.hasPath(StorageTmpPath))
    System.setProperty("java.io.tmpdir", config.getString(StorageTmpPath))

  protected def actorNameSuffix: String = node.uniqueNodeId

  private lazy val writeNodesSelectionLogic = new CapacityWriteNodesSelectionLogic(
    CapacityWriteNodesSelectionLogic.fromConfigValue(config.getString("nsdb.cluster.metrics-selector")))

  private lazy val maxAttempts = context.system.settings.config.getInt("nsdb.write.retry-attempts")

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
    context.actorOf(Props[ClusterListener], name = s"cluster-listener_${createNodeAddress(selfMember)}")

  def updateVolatileId(volatileId: String): Unit = {
    node = NSDbNode(nodeAddress, nodeFsId, volatileId)

    import context.dispatcher

    if (heartBeatDispatcher != null) heartBeatDispatcher.cancel()

    heartBeatDispatcher = context.system.scheduler.schedule(heartbeatInterval, heartbeatInterval) {
      mediator ! Publish(NODE_GUARDIANS_TOPIC, NodeAlive(node))
    }
  }

  private val clusterListener: ActorRef = createClusterListener

  private lazy val metadataCache = context.actorOf(
    Props[ReplicatedMetadataCache].withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
    s"metadata-cache_$actorNameSuffix")
  private lazy val schemaCache = context.actorOf(
    Props[ReplicatedSchemaCache].withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
    s"schema-cache_$actorNameSuffix")

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
        .props(metadataCoordinator, schemaCoordinator, mediator, new LocalityReadNodesSelection(node.uniqueNodeId))
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
      .props(indexBasePath, NSDbClusterSnapshot(context.system).getNode(nodeAddress), commitLogCoordinator)
      .withDeploy(Deploy(scope = RemoteScope(selfMember.address)))
      .withDispatcher("akka.actor.control-aware-dispatcher"),
    s"metrics-data-actor_$actorNameSuffix"
  )

  override def preStart(): Unit =
    log.info(s"NodeActorGuardian is ready at ${self.path.name}")

  def receive: Receive = {
    case GetNode =>
      sender() ! NodeGot(Option(node))
    case UpdateVolatileId(volatileId) =>
      updateVolatileId(volatileId)
      sender() ! VolatileIdUpdated(node)
    case GetNodeChildActors =>
      sender ! NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, publisherActor)
    case NodeAlive(node) =>
      import context.dispatcher
      (metadataCoordinator ? GetNodesBlackListFromCache).mapTo[GetNodesBlackListFromCacheResponse].onComplete {
        case Success(NodesBlackListFromCacheGot(blackList)) =>
          if (!blackList.contains(node)) {
            log.info(s"node $node is alive")
            NSDbClusterSnapshot(context.system).addNode(node)
          }
        case Success(GetNodesBlackListFromCacheFailed(errorMessage)) =>
        //TODO
        case Failure(exception) =>
        //TODO
      }

    case GetMetricsDataActors =>
      log.debug(s"gossiping metric data actors from node ${node.uniqueNodeId}")
      mediator ! Publish(COORDINATORS_TOPIC, SubscribeMetricsDataActor(metricsDataActor, node.uniqueNodeId))
    case GetCommitLogCoordinators =>
      log.debug(s"gossiping commit logs for node ${node.uniqueNodeId}")
      mediator ! Publish(COORDINATORS_TOPIC, SubscribeCommitLogCoordinator(commitLogCoordinator, node.uniqueNodeId))
    case GetPublishers =>
      log.debug(s"gossiping publishers for node ${node.uniqueNodeId}")
      mediator ! Publish(COORDINATORS_TOPIC, SubscribePublisher(publisherActor, node.uniqueNodeId))
  }
}

object NodeActorsGuardian {
  case class UpdateVolatileId(volatileId: String) extends NSDbSerializable
  case class VolatileIdUpdated(node: NSDbNode)    extends NSDbSerializable

  case object GetNode                        extends NSDbSerializable
  case class NodeGot(node: Option[NSDbNode]) extends NSDbSerializable
}
