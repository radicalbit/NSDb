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
import akka.cluster.metrics.{ClusterMetricsChanged, ClusterMetricsExtension, Metric}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.actor.ClusterListener.LocationsMetricChanged
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{AddLocations, RemoveNodeMetadata}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.util.{ErrorManagementUtils, FileUtils}
import io.radicalbit.nsdb.cluster.{NsdbNodeEndpoint, createNodeName}
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.model.Location.LocationWithCoordinates
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.util.ConfigKeys

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * Actor subscribed to akka cluster events. It creates all the actors needed when a node joins the cluster
  * @param nodeActorsGuardianProps props of NodeActorGuardian actor.
  */
class ClusterListener(nodeActorsGuardianProps: Props) extends Actor with ActorLogging {

  private lazy val cluster = Cluster(context.system)
  private lazy val clusterMetricSystem = ClusterMetricsExtension(context.system)
  private lazy val selfNodeName = createNodeName(cluster.selfMember)

  private val mediator = DistributedPubSub(context.system).mediator

  private val config         = context.system.settings.config
  private lazy val indexPath = config.getString(ConfigKeys.StorageIndexPath)

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val defaultTimeout: Timeout = Timeout(5.seconds)

  private val clusterMetrics: mutable.Map[(String, String), Metric] = mutable.Map.empty

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    log.info("Created ClusterListener at path {} and subscribed to member events", self.path)
    clusterMetricSystem.subscribe(self)
    mediator ! Subscribe(LOCATIONS_METRIC_TOPIC, self)

  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member) if member == cluster.selfMember =>
      log.info("Member is Up: {}", member.address)

      val nodeName = createNodeName(member)

      val nodeActorsGuardian =
        context.system.actorOf(nodeActorsGuardianProps.withDeploy(Deploy(scope = RemoteScope(member.address))),
                               name = s"guardian_$nodeName")

      (nodeActorsGuardian ? GetNodeChildActors)
        .map {
          case NodeChildActorsGot(metadataCoordinator, writeCoordinator, readCoordinator, publisherActor) =>
            mediator ! Subscribe(NODE_GUARDIANS_TOPIC, nodeActorsGuardian)

            val locationsToAdd: Seq[LocationWithCoordinates] =
              FileUtils.getLocationsFromFilesystem(indexPath, nodeName)

            val locationsGroupedBy: Map[(String, String), Seq[(String, String, Location)]] = locationsToAdd.groupBy {
              case (database, namespace, _) => (database, namespace)
            }

            Future
              .sequence {
                locationsGroupedBy.map {
                  case ((db, namespace), locations) =>
                    metadataCoordinator ? AddLocations(db, namespace, locations.map {
                      case (_, _, location) => location
                    })
                }
              }
              .map(ErrorManagementUtils.partitionResponses[LocationsAdded, AddLocationsFailed])
              .onComplete {
                case Success((_, failures)) if failures.isEmpty =>
                  new NsdbNodeEndpoint(readCoordinator, writeCoordinator, metadataCoordinator, publisherActor)(
                    context.system)
                case Success((_, failures)) =>
                  log.error(s" failures $failures")
                  cluster.leave(member.address)
                case Failure(ex) =>
                  log.error(s" failure", ex)
                  cluster.leave(member.address)
              }
          case unknownResponse =>
            log.error(s"unknown response from nodeActorsGuardian ? GetNodeChildActors $unknownResponse")
        }
    case UnreachableMember(member) =>
      log.debug("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)

      (context.actorSelection(s"/user/guardian_$selfNodeName") ? GetNodeChildActors)
        .map {
          case NodeChildActorsGot(metadataCoordinator, _, _, _) =>
            (metadataCoordinator ? RemoveNodeMetadata(createNodeName(member))).map {
              case Right(NodeMetadataRemoved(nodeName)) =>
                log.info(s"metadata successfully removed for node $nodeName")
              case Left(RemoveNodeMetadataFailed(nodeName)) =>
                log.error(s"RemoveNodeMetadataFailed for node $nodeName")
            }

          case unknownResponse =>
            log.error(s"unknown response from nodeActorsGuardian ? GetNodeChildActors $unknownResponse")
        }

    case _: MemberEvent => // ignore
//    case LocationsMetricChanged(nodeName, locations) =>
//      log.debug(s"received location metric $locations for nodeName $nodeName")
//      clusterMetrics.put((nodeName, "locations"), locations)
    case ClusterMetricsChanged(nodeMetrics) =>
      log.debug(s"received metrics $nodeMetrics")
      nodeMetrics.foreach(nodeMetric =>
        nodeMetric.metrics.foreach(metric =>
          clusterMetrics.put((createNodeName(nodeMetric.address), metric.name), metric)))
      mediator ! Publish(
        LOCATIONS_METRIC_TOPIC,
        LocationsMetricChanged(selfNodeName, FileUtils.getLocationsFromFilesystem(indexPath, selfNodeName).size))
  }
}

object ClusterListener {

  case class LocationsMetricChanged(nodeName: String, locations: Int)

  def props(nodeActorsGuardianProps: Props): Props =
    Props(new ClusterListener(nodeActorsGuardianProps))
}
