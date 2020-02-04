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

import akka.actor._
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent._
import akka.cluster.metrics.{ClusterMetricsChanged, ClusterMetricsExtension, Metric, NodeMetrics}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster._
import io.radicalbit.nsdb.cluster.actor.ClusterListener.{DiskOccupationChanged, GetNodeMetrics, NodeMetricsGot}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{AddLocations, RemoveNodeMetadata}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.metrics.NSDbMetrics
import io.radicalbit.nsdb.cluster.util.{ErrorManagementUtils, FileUtils}
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.common.protocol.NSDbSerializable
import io.radicalbit.nsdb.model.LocationWithCoordinates
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  CommitLogCoordinatorUnSubscribed,
  MetricsDataActorUnSubscribed,
  PublisherUnSubscribed
}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Actor subscribed to akka cluster events. It creates all the actors needed when a node joins the cluster
  */
class ClusterListener extends Actor with ActorLogging {

  import context.dispatcher

  private lazy val cluster             = Cluster(context.system)
  private lazy val clusterMetricSystem = ClusterMetricsExtension(context.system)
  private lazy val selfNodeName        = createNodeName(cluster.selfMember)

  private val mediator = DistributedPubSub(context.system).mediator

  private lazy val config    = context.system.settings.config
  private lazy val indexPath = config.getString(StorageIndexPath)

  implicit val defaultTimeout: Timeout = Timeout(5.seconds)

  /**
  collects all the metrics coming from the akka metric system collector.
    */
  private var akkaClusterMetrics: Map[String, Set[NodeMetrics]] = Map.empty

  /**
  collects all the high level metrics (e.g. disk occupation ratio)
    */
  private val nsdbMetrics: mutable.Map[String, Set[NodeMetrics]] = mutable.Map.empty

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    log.info("Created ClusterListener at path {} and subscribed to member events", self.path)
    clusterMetricSystem.subscribe(self)
    mediator ! Subscribe(NSDB_METRICS_TOPIC, self)

  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  protected def createNodeActorsGuardian(): ActorRef =
    context.system.actorOf(
      NodeActorsGuardian.props(self).withDeploy(Deploy(scope = RemoteScope(cluster.selfMember.address))),
      name = s"guardian_$selfNodeName"
    )

  protected def retrieveLocationsToAdd() =
    FileUtils.getLocationsFromFilesystem(indexPath, selfNodeName)

  protected def handleF(f: Future[(List[LocationsAdded], List[AddLocationsFailed])],
                        readCoordinator: ActorRef,
                        writeCoordinator: ActorRef,
                        metadataCoordinator: ActorRef,
                        publisherActor: ActorRef,
                        member: Member) =
    f onComplete {
      case Success((_, failures)) if failures.isEmpty =>
        new NsdbNodeEndpoint(readCoordinator, writeCoordinator, metadataCoordinator, publisherActor)(context.system)
      case Success((_, failures)) =>
        log.error(s" failures $failures")
        cluster.leave(member.address)
      case Failure(ex) =>
        log.error(s" failure", ex)
        cluster.leave(member.address)
    }

  def receive: Receive = {
    case MemberUp(member) if member == cluster.selfMember =>
      log.info("Member is Up: {}", member.address)

      val nodeName = createNodeName(member)

      val nodeActorsGuardian = createNodeActorsGuardian()

      (nodeActorsGuardian ? GetNodeChildActors)
        .map {
          case NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, publisherActor) =>
            mediator ! Subscribe(NODE_GUARDIANS_TOPIC, nodeActorsGuardian)

            val locationsToAdd: Seq[LocationWithCoordinates] = retrieveLocationsToAdd()

            val locationsGroupedBy: Map[(String, String), Seq[LocationWithCoordinates]] = locationsToAdd.groupBy {
              case LocationWithCoordinates(database, namespace, _) => (database, namespace)
            }

            val f: Future[(List[LocationsAdded], List[AddLocationsFailed])] =
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

            handleF(f, readCoordinator, writeCoordinator, metadataCoordinator, publisherActor, member)
          case unknownResponse =>
            log.error(s"unknown response from nodeActorsGuardian ? GetNodeChildActors $unknownResponse")
        }
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)

      val nodeName = createNodeName(member)

      (for {
        NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, _) <- (context.actorSelection(
          s"/user/guardian_$selfNodeName") ? GetNodeChildActors).mapTo[NodeChildActorsGot]
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
      } yield removeNodeMetadataResponse).map {
        case NodeMetadataRemoved(nodeName) =>
          log.info(s"metadata successfully removed for node $nodeName")
        case RemoveNodeMetadataFailed(nodeName) =>
          log.error(s"RemoveNodeMetadataFailed for node $nodeName")
      }

    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
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
        // if the fs path has not been created yet, the occupation ratio will be 100.0
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

object ClusterListener {

  /**
    * Event fired when akka cluster metrics are collected that described the disk occupation ration for a node
    * @param nodeName cluster node name.
    * @param usableSpace the free space on disk.
    * @param totalSpace total disk space.
    */
  case class DiskOccupationChanged(nodeName: String, usableSpace: Long, totalSpace: Long) extends NSDbSerializable

  case object GetNodeMetrics extends NSDbSerializable

  /**
    * Contains the metrics for each alive member of the cluster.
    * @param nodeMetrics one entry contains all the metrics for a single node.
    */
  case class NodeMetricsGot(nodeMetrics: Set[NodeMetrics]) extends NSDbSerializable

}
