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

import java.nio.file.{Files, NoSuchFileException, Paths}
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.metrics.{ClusterMetricsChanged, ClusterMetricsExtension, Metric, NodeMetrics}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.cluster.{Cluster, Member}
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster._
import io.radicalbit.nsdb.cluster.actor.NSDbMetricsEvents._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{AddLocations, RemoveNodeMetadata}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.cluster.metrics.NSDbMetrics
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.model.LocationWithCoordinates
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  CommitLogCoordinatorUnSubscribed,
  MetricsDataActorUnSubscribed,
  PublisherUnSubscribed
}
import io.radicalbit.nsdb.util.{ErrorManagementUtils, FileUtils, FutureRetryUtility}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Try}

/**
  * Actor subscribed to akka cluster events. It creates all the actors needed when a node joins the cluster
  */
abstract class AbstractClusterListener extends Actor with ActorLogging with FutureRetryUtility {

  import context.dispatcher

  protected lazy val cluster           = Cluster(context.system)
  private lazy val clusterMetricSystem = ClusterMetricsExtension(context.system)
  protected lazy val selfNodeName      = createNodeName(cluster.selfMember)
  protected lazy val nodeId            = FileUtils.getOrCreateNodeId(selfNodeName, config.getString(NSDBMetadataPath))

  private val mediator = DistributedPubSub(context.system).mediator

  private lazy val config    = context.system.settings.config
  private lazy val indexPath = config.getString(StorageIndexPath)

  implicit val defaultTimeout: Timeout =
    Timeout(config.getDuration(globalTimeout, TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
  collects all the metrics coming from the akka metric system collector.
    */
  private var akkaClusterMetrics: Map[String, Set[NodeMetrics]] = Map.empty

  /**
  collects all the high level metrics (e.g. disk occupation ratio)
    */
  private val nsdbMetrics: mutable.Map[String, Set[NodeMetrics]] = mutable.Map.empty

  /**
    * Retry policy
    */
  private lazy val delay   = FiniteDuration(config.getDuration(retryPolicyDelay, TimeUnit.SECONDS), TimeUnit.SECONDS)
  private lazy val retries = config.getInt(retryPolicyNRetries)

  implicit val scheduler: Scheduler = context.system.scheduler
  implicit val _log: LoggingAdapter = log

  def enableClusterMetricsExtension: Boolean

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    log.info("Created ClusterListener at path {} and subscribed to member events", self.path)
    if (enableClusterMetricsExtension) clusterMetricSystem.subscribe(self)
    mediator ! Subscribe(NSDB_METRICS_TOPIC, self)
    mediator ! Subscribe(NSDB_LISTENERS_TOPIC, self)
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  protected def createNodeActorsGuardian(): ActorRef = {
    context.system.actorOf(
      NodeActorsGuardian.props(self, nodeId).withDeploy(Deploy(scope = RemoteScope(cluster.selfMember.address))),
      name = s"guardian_${selfNodeName}"
    )
  }

  protected def retrieveLocationsToAdd: List[LocationWithCoordinates] =
    FileUtils.getLocationsFromFilesystem(indexPath, selfNodeName)

  protected def onSuccessBehaviour(readCoordinator: ActorRef,
                                   writeCoordinator: ActorRef,
                                   metadataCoordinator: ActorRef,
                                   publisherActor: ActorRef): Unit

  protected def onFailureBehaviour(member: Member, error: Any): Unit

  protected def onRemoveNodeMetadataResponse: RemoveNodeMetadataResponse => Unit = {
    case NodeMetadataRemoved(nodeName) =>
      log.info(s"metadata successfully removed for node $nodeName")
    case RemoveNodeMetadataFailed(nodeName) =>
      log.error(s"RemoveNodeMetadataFailed for node $nodeName")
  }

  private def unsubscribeNode(nodeName: String)(implicit scheduler: Scheduler, _log: LoggingAdapter) = {
    (for {
      NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, _) <- (context.actorSelection(
        s"/user/guardian_$nodeName") ? GetNodeChildActors)
        .mapTo[NodeChildActorsGot]
      _ <- (readCoordinator ? UnsubscribeMetricsDataActor(nodeName)).mapTo[MetricsDataActorUnSubscribed]
      _ <- (writeCoordinator ? UnSubscribeCommitLogCoordinator(nodeName))
        .mapTo[CommitLogCoordinatorUnSubscribed]
      _ <- (writeCoordinator ? UnSubscribePublisher(nodeName)).mapTo[PublisherUnSubscribed]
      _ <- (writeCoordinator ? UnsubscribeMetricsDataActor(nodeName))
        .mapTo[MetricsDataActorUnSubscribed]
      _ <- (metadataCoordinator ? UnsubscribeMetricsDataActor(nodeName))
        .mapTo[MetricsDataActorUnSubscribed]
      _ <- (metadataCoordinator ? UnSubscribeCommitLogCoordinator(nodeName))
        .mapTo[CommitLogCoordinatorUnSubscribed]
      removeNodeMetadataResponse <- (metadataCoordinator ? RemoveNodeMetadata(nodeName))
        .mapTo[RemoveNodeMetadataResponse]
    } yield removeNodeMetadataResponse)
      .retry(delay, retries)(_.isInstanceOf[NodeMetadataRemoved])
      .map(onRemoveNodeMetadataResponse)
  }

  def receive: Receive = {
    case MemberUp(member) if member == cluster.selfMember =>
      log.info("Member is Up: {}", member.address)

      val nodeActorsGuardian = createNodeActorsGuardian()

      (nodeActorsGuardian ? GetNodeChildActors)
        .map {
          case NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, publisherActor) =>
            val locationsToAdd: Seq[LocationWithCoordinates] = retrieveLocationsToAdd

            val locationsGroupedBy: Map[(String, String), Seq[LocationWithCoordinates]] = locationsToAdd.groupBy {
              case LocationWithCoordinates(database, namespace, _) => (database, namespace)
            }

            implicit val scheduler: Scheduler = context.system.scheduler
            implicit val _log: LoggingAdapter = log

            Future
              .sequence {
                locationsGroupedBy.map {
                  case ((db, namespace), locations) =>
                    metadataCoordinator ? AddLocations(db, namespace, locations.map {
                      case LocationWithCoordinates(_, _, location) => location
                    })
                }
              }
              .map(ErrorManagementUtils.partitionResponses[LocationsAdded, AddLocationsFailed])
              .retry(delay, retries) {
                case (_, addLocationsFailedList) => addLocationsFailedList.isEmpty
              }
              .onComplete {
                case Success((_, failures)) if failures.isEmpty =>
                  val nodeName = createNodeName(member)
                  mediator ! Subscribe(NODE_GUARDIANS_TOPIC, nodeActorsGuardian)
                  mediator ! Publish(NSDB_LISTENERS_TOPIC, NodeAlive(nodeId, nodeName))
                  NSDbClusterSnapshot(context.system).addNode(nodeId, nodeName)
                  onSuccessBehaviour(readCoordinator, writeCoordinator, metadataCoordinator, publisherActor)
                case e =>
                  onFailureBehaviour(member, e)
              }
          case unknownResponse =>
            log.error(s"unknown response from nodeActorsGuardian ? GetNodeChildActors $unknownResponse")
            context.system.terminate()
        }
    case NodeAlive(nodeId, address) =>
      NSDbClusterSnapshot(context.system).addNode(nodeId, address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)

      val nodeName = createNodeName(member)

      unsubscribeNode(nodeName)

      NSDbClusterSnapshot(context.system).removeNode(nodeName)

    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)

      val nodeName = createNodeName(member)

      unsubscribeNode(nodeName)

      NSDbClusterSnapshot(context.system).removeNode(nodeName)
    case _: MemberEvent => // ignore
    case DiskOccupationChanged(nodeName, usableSpace, totalSpace) =>
      log.debug(s"received usableSpace $usableSpace and totalSpace $totalSpace for nodeName $nodeName")
      nsdbMetrics.put(
        nodeName,
        Set(
          NodeMetrics(
            createAddress(nodeName),
            System.currentTimeMillis(),
            Set(Metric(NSDbMetrics.DiskFreeSpace, usableSpace, None),
                Metric(NSDbMetrics.DiskTotalSpace, totalSpace, None))
          ))
      )
      log.debug(s"nsdb metrics $nsdbMetrics")
    case ClusterMetricsChanged(nodeMetrics) =>
      log.debug(s"received metrics $nodeMetrics")
      akkaClusterMetrics = nodeMetrics.groupBy(nodeMetric => createNodeName(nodeMetric.address))
      Try {
        val fs = Files.getFileStore(Paths.get(indexPath))
        mediator ! Publish(NSDB_METRICS_TOPIC, DiskOccupationChanged(selfNodeName, fs.getUsableSpace, fs.getTotalSpace))
        log.debug(s"akka cluster metrics $akkaClusterMetrics")
      }.recover {
        /* if the fs path has not been created yet, we assume that the disk is fully available.
          A convenient value is set both for usable space and total space.
         */
        case _: NoSuchFileException =>
          mediator ! Publish(NSDB_METRICS_TOPIC, DiskOccupationChanged(selfNodeName, 100, 100))
      }
    case GetNodeMetrics =>
      val mergedMetrics = (akkaClusterMetrics ++ nsdbMetrics).values.map(nodeMetricsSet =>
        nodeMetricsSet.reduce { (nodeMetrics1: NodeMetrics, nodeMetrics2: NodeMetrics) =>
          NodeMetrics(nodeMetrics1.address,
                      System.currentTimeMillis(),
                      metrics = nodeMetrics1.metrics ++ nodeMetrics2.metrics)
      })
      sender() ! NodeMetricsGot(mergedMetrics.toSet)
  }
}
