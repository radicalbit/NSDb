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
import io.radicalbit.nsdb.cluster.actor.NodeActorsGuardian.UpdateVolatileId
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
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

  protected val cluster: Cluster     = Cluster(context.system)
  private val clusterMetricSystem    = ClusterMetricsExtension(context.system)
  protected val selfNodeName: String = createNodeAddress(cluster.selfMember)
  protected val nodeFsId: String     = FileUtils.getOrCreateNodeFsId(selfNodeName, config.getString(NSDBMetadataPath))

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
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[ReachabilityEvent])
    log.info("Created ClusterListener at path {} and subscribed to member events", self.path)
    if (enableClusterMetricsExtension) clusterMetricSystem.subscribe(self)
    mediator ! Subscribe(NSDB_METRICS_TOPIC, self)
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

  private def blacklistNode(otherNode: NSDbNode)(implicit scheduler: Scheduler, _log: LoggingAdapter) = {
    log.info(s"blacklist node $otherNode from node $nodeFsId")
    (for {
      NodeChildActorsGot(metadataCoordinator, _, _, _) <- (context.parent ? GetNodeChildActors)
        .mapTo[NodeChildActorsGot]
      blackListResponse <- (metadataCoordinator ? AddNodeToBlackList(otherNode)).mapTo[AddNodeToBlackListResponse]
    } yield blackListResponse)
      .retry(delay, retries)(_.isInstanceOf[NodeToBlackListAdded])
  }

  private def whitelistNode(address: String)(implicit scheduler: Scheduler, _log: LoggingAdapter) = {
    log.info(s"whitelist node address $address")
    (for {
      NodeChildActorsGot(metadataCoordinator, _, _, _) <- (context.parent ? GetNodeChildActors)
        .mapTo[NodeChildActorsGot]
      blackListResponse <- (metadataCoordinator ? RemoveNodeFromBlackList(address))
        .mapTo[RemoveNodeFromBlacklistResponse]
    } yield blackListResponse)
      .retry(delay, retries)(_.isInstanceOf[NodeFromBlacklistRemoved])
  }

  private def unsubscribeNode(otherNode: NSDbNode)(implicit scheduler: Scheduler, _log: LoggingAdapter) = {
    log.info(s"unsubscribing node $otherNode from node $nodeFsId")
    val otherNodeId = otherNode.uniqueNodeId
    (for {
      NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, _) <- (context.parent ? GetNodeChildActors)
        .mapTo[NodeChildActorsGot]
      _ <- metadataCoordinator ? AddNodeToBlackList(otherNode)
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
      log.info(s"Member with nodeId $nodeFsId and address ${member.address} is Up")

      val volatileId = RandomStringUtils.randomAlphabetic(VOLATILE_ID_LENGTH)

      val node = NSDbNode(createNodeAddress(member), nodeFsId, volatileId)

      val nodeActorsGuardian = context.parent
      (for {
        _ <- nodeActorsGuardian ? UpdateVolatileId(volatileId)
        children @ NodeChildActorsGot(metadataCoordinator, _, _, _) <- (nodeActorsGuardian ? GetNodeChildActors)
          .mapTo[NodeChildActorsGot]
        outdatedLocations <- (children.metadataCoordinator ? GetOutdatedLocations).mapTo[OutdatedLocationsGot]
        addLocationsResult <- {

          val locationsToAdd: Seq[LocationWithCoordinates] =
            retrieveLocationsToAdd(node).diff(outdatedLocations.locations)

          log.info(s"locations to add from node $nodeFsId \t$locationsToAdd")

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
            log.info(s"location ${success} successfully added for node $nodeFsId")

            mediator ! Subscribe(NODE_GUARDIANS_TOPIC, nodeActorsGuardian)
            mediator ! Publish(NODE_GUARDIANS_TOPIC, NodeAlive(node))
            NSDbClusterSnapshot(context.system).addNode(node)
            onSuccessBehaviour(readCoordinator, writeCoordinator, metadataCoordinator, publisherActor)
          case e =>
            onFailureBehaviour(member, e)
        }
    case UnreachableMember(member) if member != cluster.selfMember =>
      val nodeAddress = createNodeAddress(member)
      log.warning(s"Member detected as unreachable: $member")

      NSDbClusterSnapshot(context.system).getNode(createNodeAddress(member)) match {
        case Some(nodeToRemove) =>
          log.info(s"member $member is mapped into $nodeToRemove")
          blacklistNode(nodeToRemove)
        case None =>
          log.error("member $member has no mapping")
      }

      NSDbClusterSnapshot(context.system).removeNode(nodeAddress)

    case ReachableMember(member) =>
      log.warning(s"Member detected as reachable: $member ")

      whitelistNode(createNodeAddress(member))

    case MemberRemoved(member, previousStatus) if member != cluster.selfMember =>
      log.warning("{} Member is Removed: {} after {}", selfNodeName, member.address, previousStatus)
      val nodeAddress = createNodeAddress(member)

      NSDbClusterSnapshot(context.system).getNode(createNodeAddress(member)) match {
        case Some(nodeToRemove) =>
          log.info(s"member $member is mapped into $nodeToRemove")
          unsubscribeNode(nodeToRemove)
        case None =>
          log.error("member $member has no mapping")
      }

      NSDbClusterSnapshot(context.system).removeNode(nodeAddress)
    case event: MemberEvent =>
      log.debug(s"received cluster event $event")
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
