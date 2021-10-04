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

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.metrics.{ClusterMetricsChanged, ClusterMetricsExtension, Metric, NodeMetrics}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.cluster.{Cluster, Member}
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster._
import io.radicalbit.nsdb.cluster.actor.NSDbMetricsEvents._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{
  AddLocations,
  GetOutdatedLocations,
  RemoveNodeMetadata
}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.cluster.logic.WriteConfig.MetadataConsistency
import io.radicalbit.nsdb.cluster.metrics.NSDbMetrics
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.common.protocol.NSDbNode
import io.radicalbit.nsdb.model.LocationWithCoordinates
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  CommitLogCoordinatorUnSubscribed,
  MetricsDataActorUnSubscribed,
  PublisherUnSubscribed
}
import io.radicalbit.nsdb.util.FileUtils.NODE_ID_LENGTH
import io.radicalbit.nsdb.util.{ErrorManagementUtils, FileUtils, FutureRetryUtility}
import org.apache.commons.lang3.RandomStringUtils

import java.nio.file.{Files, NoSuchFileException, Paths}
import java.util.concurrent.TimeUnit
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
  protected lazy val selfNodeName      = createNodeAddress(cluster.selfMember)
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

  private var nodeUuid: String = _

  def enableClusterMetricsExtension: Boolean

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    log.info("Created ClusterListener at path {} and subscribed to member events", self.path)
    if (enableClusterMetricsExtension) clusterMetricSystem.subscribe(self)
    mediator ! Subscribe(NSDB_METRICS_TOPIC, self)
    mediator ! Subscribe(NSDB_LISTENERS_TOPIC, self)
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  protected def retrieveLocationsToAdd(node: NSDbNode): List[LocationWithCoordinates] =
    FileUtils.getLocationsFromFilesystem(indexPath, node)

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

  private def unsubscribeNode(otherNodeId: String)(implicit scheduler: Scheduler, _log: LoggingAdapter) = {
    log.info(s"unsubscribing node $otherNodeId from node $nodeId")
    (for {
      NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, _) <- (context.parent ? GetNodeChildActors)
        .mapTo[NodeChildActorsGot]
      _ <- (readCoordinator ? UnsubscribeMetricsDataActor(otherNodeId)).mapTo[MetricsDataActorUnSubscribed]
      _ <- (writeCoordinator ? UnSubscribeCommitLogCoordinator(otherNodeId))
        .mapTo[CommitLogCoordinatorUnSubscribed]
      _ <- (writeCoordinator ? UnSubscribePublisher(otherNodeId)).mapTo[PublisherUnSubscribed]
      _ <- (writeCoordinator ? UnsubscribeMetricsDataActor(otherNodeId))
        .mapTo[MetricsDataActorUnSubscribed]
      _ <- (metadataCoordinator ? UnsubscribeMetricsDataActor(otherNodeId))
        .mapTo[MetricsDataActorUnSubscribed]
      _ <- (metadataCoordinator ? UnSubscribeCommitLogCoordinator(otherNodeId))
        .mapTo[CommitLogCoordinatorUnSubscribed]
      removeNodeMetadataResponse <- (metadataCoordinator ? RemoveNodeMetadata(otherNodeId))
        .mapTo[RemoveNodeMetadataResponse]
    } yield removeNodeMetadataResponse)
      .retry(delay, retries)(_.isInstanceOf[NodeMetadataRemoved])
      .map(onRemoveNodeMetadataResponse)
  }

  def receive: Receive = {
    case MemberUp(member) if member == cluster.selfMember =>
      log.info(s"Member with nodeId $nodeId and address ${member.address} is Up")

      val nodeAddress = createNodeAddress(member)

      val node = NSDbNode(nodeAddress, nodeId)

      val nodeActorsGuardian = context.parent
      (for {
        children @ NodeChildActorsGot(metadataCoordinator, _, _, _) <- (nodeActorsGuardian ? GetNodeChildActors)
          .mapTo[NodeChildActorsGot]
        outdatedLocations <- (children.metadataCoordinator ? GetOutdatedLocations).mapTo[OutdatedLocationsGot]
        addLocationsResult <- {

          val locationsToAdd: Seq[LocationWithCoordinates] =
            retrieveLocationsToAdd(node).diff(outdatedLocations.locations)

          log.info(s"locations to add from node $nodeId \t$locationsToAdd")

          val locationsGroupedBy: Map[(String, String), Seq[LocationWithCoordinates]] = locationsToAdd.groupBy {
            case LocationWithCoordinates(database, namespace, _) => (database, namespace)
          }

          Future
            .sequence {
              locationsGroupedBy.map {
                case ((db, namespace), locations) =>
                  metadataCoordinator ? AddLocations(db, namespace, locations.map {
                    case LocationWithCoordinates(_, _, location) => location
                  }, consistency = Some(MetadataConsistency.Local))
              }
            }
            .map(ErrorManagementUtils.partitionResponses[LocationsAdded, AddLocationsFailed])
            .retry(delay, retries) {
              case (_, addLocationsFailedList) => addLocationsFailedList.isEmpty
            }
        }
      } yield (children, addLocationsResult))
        .onComplete {
          case Success(
              (NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, publisherActor),
               (success, failures))) if failures.isEmpty =>
            log.info(s"location ${success} successfully added for node $nodeId")

            val interval =
              FiniteDuration(context.system.settings.config.getDuration("nsdb.heartbeat.interval", TimeUnit.SECONDS),
                             TimeUnit.SECONDS)

            context.system.scheduler.schedule(interval, interval) {
              mediator ! Publish(NSDB_LISTENERS_TOPIC, NodeAlive(node))
            }

            mediator ! Subscribe(NODE_GUARDIANS_TOPIC, nodeActorsGuardian)
            mediator ! Publish(NSDB_LISTENERS_TOPIC, NodeAlive(node))
            NSDbClusterSnapshot(context.system).addNode(node)
            onSuccessBehaviour(readCoordinator, writeCoordinator, metadataCoordinator, publisherActor)
          case e =>
            onFailureBehaviour(member, e)
        }
    case NodeAlive(node) =>
      NSDbClusterSnapshot(context.system).addNode(node)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) if member != cluster.selfMember =>
      log.warning("{} Member is Removed: {} after {}", selfNodeName, member.address, previousStatus)

      val nodeName       = createNodeAddress(member)
      val nodeIdToRemove = NSDbClusterSnapshot(context.system).getNode(nodeName)

      unsubscribeNode(nodeIdToRemove.nodeFsId)

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
      akkaClusterMetrics = nodeMetrics.groupBy(nodeMetric => createNodeAddress(nodeMetric.address))
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
