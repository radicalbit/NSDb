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

package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.pattern._
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.MetadataActor.MetricLocations
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{
  AddLocationFailed,
  LocationAdded,
  LocationGot,
  LocationsGot
}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.WarmUpCompleted

import scala.concurrent.Future
import scala.util.Random

/**
  * Actor that handles metadata (i.e. write location for metrics)
  * @param cache cluster aware metric's location cache
  */
class MetadataCoordinator(cache: ActorRef) extends Actor with ActorLogging with Stash {

  /**
    * mediator for [[DistributedPubSub]] system
    */
  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  val cluster = Cluster(context.system)

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.metadata-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  lazy val shardingInterval: Long = context.system.settings.config.getDuration("nsdb.sharding.interval").toMillis

  override def receive: Receive = warmUp

  def warmUp: Receive = {
    case msg @ WarmUpLocations(metricsLocations) =>
      log.debug(s"Received location warm-up message: $msg ")
      metricsLocations.foreach { metricLocations =>
        metricLocations.locations.foreach { location =>
          val db        = metricLocations.db
          val namespace = metricLocations.namespace
          (cache ? PutInCache(LocationKey(metricLocations.db,
                                          metricLocations.namespace,
                                          metricLocations.metric,
                                          location.from,
                                          location.to),
                              location))
            .map {
              case Cached(_, Some(_)) =>
                LocationAdded(db, namespace, location)
              case _ => AddLocationFailed(db, namespace, location)
            }
        }
      }

      mediator ! Publish("warm-up", WarmUpCompleted)
      unstashAll()
      context.become(shardBehaviour)

    case msg => stash()
  }

  /**
    * behaviour in case shard is true
    */
  def shardBehaviour: Receive = {
    case GetLocations(db, namespace, metric) =>
      val f = (cache ? GetLocationsFromCache(MetricKey(db, namespace, metric)))
        .mapTo[CachedLocations]
        .map(l => LocationsGot(db, namespace, metric, l.value))
      f.pipeTo(sender())
    case GetWriteLocation(db, namespace, metric, timestamp) =>
      val startShard = (timestamp / shardingInterval) * shardingInterval
      val endShard   = startShard + shardingInterval
      val nodeName =
        s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"
      (cache ? GetLocationsFromCache(MetricKey(db, namespace, metric)))
        .flatMap {
          case CachedLocations(_, values) if values.nonEmpty =>
            values.find(v => v.from <= timestamp && v.to >= timestamp) match {
              case Some(loc) => Future(LocationGot(db, namespace, metric, Some(loc)))
              case None =>
                (self ? AddLocation(db,
                                    namespace,
                                    Location(metric, nodeName, startShard, endShard)))
                  .map {
                    case LocationAdded(_, _, location) => LocationGot(db, namespace, metric, Some(location))
                    case AddLocationFailed(_, _, _)    => LocationGot(db, namespace, metric, None)
                  }
            }

          case CachedLocations(_, _) =>
            (self ? AddLocation(db, namespace, Location(metric, nodeName, startShard, endShard)))
              .map {
                case LocationAdded(_, _, location) => LocationGot(db, namespace, metric, Some(location))
                case AddLocationFailed(_, _, _)    => LocationGot(db, namespace, metric, None)
              }

        }
        .pipeTo(sender)

    case msg @ AddLocation(db, namespace, location) =>
      (cache ? PutInCache(LocationKey(db, namespace, location.metric, location.from, location.to), location))
        .map {
          case Cached(_, Some(_)) =>
            mediator ! Publish("metadata", msg)
            LocationAdded(db, namespace, location)
          case _ => AddLocationFailed(db, namespace, location)
        }
        .pipeTo(sender)
  }
}

object MetadataCoordinator {

  object commands {

    case class WarmUpLocations(metricLocations: Seq[MetricLocations])
    case class GetLocations(db: String, namespace: String, metric: String)
    case class GetWriteLocation(db: String, namespace: String, metric: String, timestamp: Long)
    case class AddLocation(db: String, namespace: String, location: Location)
    case class AddLocations(db: String, namespace: String, locations: Seq[Location])
    case class DeleteLocation(db: String, namespace: String, location: Location)
    case class DeleteNamespace(db: String, namespace: String, occurredOn: Long = System.currentTimeMillis)
  }

  object events {

    case class LocationsGot(db: String, namespace: String, metric: String, locations: Seq[Location])
    case class LocationGot(db: String, namespace: String, metric: String, location: Option[Location])
    case class UpdateLocationFailed(db: String, namespace: String, oldLocation: Location, newOccupation: Long)
    case class LocationAdded(db: String, namespace: String, location: Location)
    case class AddLocationFailed(db: String, namespace: String, location: Location)
    case class LocationsAdded(db: String, namespace: String, locations: Seq[Location])
    case class LocationDeleted(db: String, namespace: String, location: Location)
    case class NamespaceDeleted(db: String, namespace: String, occurredOn: Long)
  }

  def props(cache: ActorRef): Props = Props(new MetadataCoordinator(cache))
}
