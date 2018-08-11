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
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.cluster.{Cluster, MemberStatus}
import akka.pattern._
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.actor.MetadataActor.MetricMetadata
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.createNodeName
import io.radicalbit.nsdb.cluster.index.{Location, MetricInfo}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.WarmUpCompleted
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.concurrent.Future

/**
  * Actor that handles metadata (i.e. write location for metrics)
  * @param cache cluster aware metric's location cache
  */
class MetadataCoordinator(cache: ActorRef, mediator: ActorRef) extends ActorPathLogging with Stash {
  val cluster = Cluster(context.system)

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.metadata-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  lazy val defaultShardingInterval: Long =
    context.system.settings.config.getDuration("nsdb.sharding.interval").toMillis

  lazy val replicationFactor: Int =
    context.system.settings.config.getInt("nsdb.cluster.replication-factor")

  override def receive: Receive = warmUp

  def warmUp: Receive = {
    case msg @ WarmUpMetadata(metricsMetadata) =>
      log.info(s"Received location warm-up message: $msg ")
      Future
        .sequence(metricsMetadata.map { metadata =>
          Future
            .sequence(metadata.locations.map { location =>
              (cache ? PutLocationInCache(metadata.db,
                                          metadata.namespace,
                                          metadata.metric,
                                          location.from,
                                          location.to,
                                          location))
                .mapTo[LocationCached]
            })
            .flatMap { _ =>
              metadata.info.map(metricInfo =>
                (cache ? PutMetricInfoInCache(metadata.db, metadata.namespace, metricInfo.metric, metricInfo))
                  .mapTo[MetricInfoCached]) getOrElse Future(MetricInfoGot(metadata.db, metadata.namespace, None))
            }
        })
        .recover {
          case t =>
            log.error(t, "error during warm up")
            context.system.terminate()
        }
        .foreach { _ =>
          mediator ! Publish(WARMUP_TOPIC, WarmUpCompleted)
          unstashAll()
          context.become(operative)
        }

    case msg =>
      stash()
      log.error(s"Received and stashed message $msg during warmUp")
  }

  private def getShardStartIstant(timestamp: Long, shardInterval: Long) = (timestamp / shardInterval) * shardInterval

  private def getShardEndIstant(startShard: Long, shardInterval: Long) = startShard + shardInterval

  private def performAddLocationIntoCache(db: String, namespace: String, locations: Seq[Location]) =
    Future
      .sequence(locations.map(location =>
        cache ? PutLocationInCache(db, namespace, location.metric, location.from, location.to, location)))
      .flatMap {
        case locations: Seq[LocationCached] =>
          locations.foreach(l => mediator ! Publish(METADATA_TOPIC, AddLocation(db, namespace, l.value)))
          Future(LocationsAdded(db, namespace, locations.map(_.value)))
        //some error occurred
        case results: Seq[_] =>
          val successToBeEvicted = results.collect {
            case e: LocationCached => e.value
          }
          val errors = results.collect {
            case e: PutLocationInCacheFailed => e.location
          }
          Future
            .sequence(successToBeEvicted.map(location => cache ? EvictLocation(db, namespace, location)))
            .flatMap { _ =>
              Future(AddLocationsFailed(db, namespace, errors))
            }
      }

  /**
    * Retrieve the actual shard interval for a metric. If a custom interval has been configured, it will be returned, otherwise the default interval (gather from the global conf file) will be used
    * @param db the db.
    * @param namespace the namespace.
    * @param metric the metric.
    * @return the actual shard interval.
    */
  private def getShardInterval(db: String, namespace: String, metric: String): Future[Long] =
    (cache ? GetMetricInfoFromCache(db, namespace, metric))
      .flatMap {
        case MetricInfoCached(_, _, _, Some(metricInfo)) => Future(metricInfo.shardInterval)
        case _ =>
          (cache ? PutMetricInfoInCache(db, namespace, metric, MetricInfo(metric, defaultShardingInterval)))
            .map(_ => defaultShardingInterval)
      }

  def operative: Receive = {
    case GetLocations(db, namespace, metric) =>
      (cache ? GetLocationsFromCache(db, namespace, metric))
        .mapTo[LocationsCached]
        .map(l => LocationsGot(db, namespace, metric, l.value))
        .pipeTo(sender())
    case GetWriteLocations(db, namespace, metric, timestamp) =>
      (cache ? GetLocationsFromCache(db, namespace, metric))
        .flatMap {
          case LocationsCached(_, _, _, values) if values.nonEmpty =>
            values.filter(v => v.from <= timestamp && v.to >= timestamp) match {
              case Nil =>
                getShardInterval(db, namespace, metric)
                  .flatMap { interval =>
                    val start = getShardStartIstant(timestamp, interval)
                    val end   = getShardEndIstant(start, interval)

                    //TODO more sophisticated node choice logic must be added here
                    val nodes = cluster.state.members
                      .filter(_.status == MemberStatus.Up)
                      .take(replicationFactor)
                      .map(createNodeName)

                    val locations = nodes.map(Location(metric, _, start, end)).toSeq

                    performAddLocationIntoCache(db, namespace, locations).map {
                      case LocationsAdded(_, _, locations) => LocationsGot(db, namespace, metric, locations)
                      case _                               => GetWriteLocationsFailed(db, namespace, metric, timestamp)
                    }
                  }
              case s => Future(LocationsGot(db, namespace, metric, s))
            }
          case LocationsCached(_, _, _, _) =>
            getShardInterval(db, namespace, metric)
              .flatMap { interval =>
                val start = getShardStartIstant(timestamp, interval)
                val end   = getShardEndIstant(start, interval)

                val nodes = cluster.state.members
                  .filter(_.status == MemberStatus.Up)
                  .take(replicationFactor)
                  .map(createNodeName)

                val locations = nodes.map(Location(metric, _, start, end)).toSeq

                performAddLocationIntoCache(db, namespace, locations).map {
                  case LocationsAdded(_, _, locations) => LocationsGot(db, namespace, metric, locations)
                  case _                               => GetWriteLocationsFailed(db, namespace, metric, timestamp)
                }
              }
          case _ =>
            Future(LocationsGot(db, namespace, metric, Seq.empty))
        } pipeTo sender()
    case AddLocation(db, namespace, location) =>
      performAddLocationIntoCache(db, namespace, Seq(location)).pipeTo(sender)
    case GetMetricInfo(db, namespace, metric) =>
      (cache ? GetMetricInfoFromCache(db, namespace, metric))
        .map {
          case MetricInfoCached(_, _, _, value) => MetricInfoGot(db, namespace, value)
        }
        .pipeTo(sender)
    case msg @ PutMetricInfo(db, namespace, metricInfo) =>
      (cache ? PutMetricInfoInCache(db, namespace, metricInfo.metric, metricInfo))
        .map {
          case MetricInfoCached(_, _, _, Some(_)) =>
            mediator ! Publish(METADATA_TOPIC, msg)
            MetricInfoPut(db, namespace, metricInfo)
          case MetricInfoAlreadyExisting(_, _) =>
            MetricInfoFailed(db, namespace, metricInfo, "metric info already exist")
          case e => MetricInfoFailed(db, namespace, metricInfo, s"Unknown response from cache $e")
        }
        .pipeTo(sender)
  }
}

object MetadataCoordinator {

  object commands {

    case class WarmUpMetadata(metricLocations: Seq[MetricMetadata])
    case class GetLocations(db: String, namespace: String, metric: String)
    case class GetWriteLocations(db: String, namespace: String, metric: String, timestamp: Long)
    case class AddLocation(db: String, namespace: String, location: Location)
    case class AddLocations(db: String, namespace: String, locations: Seq[Location])
    case class DeleteLocation(db: String, namespace: String, location: Location)
    case class DeleteNamespace(db: String, namespace: String, occurredOn: Long = System.currentTimeMillis)

    case class GetMetricInfo(db: String, namespace: String, metric: String)
    case class PutMetricInfo(db: String, namespace: String, metricInfo: MetricInfo)
  }

  object events {

    case class LocationsGot(db: String, namespace: String, metric: String, locations: Seq[Location])
    case class GetWriteLocationsFailed(db: String, namespace: String, metric: String, timestamp: Long)
    case class UpdateLocationFailed(db: String, namespace: String, oldLocation: Location, newOccupation: Long)
    case class LocationAdded(db: String, namespace: String, location: Location)
    case class AddLocationFailed(db: String, namespace: String, location: Location)
    case class LocationsAdded(db: String, namespace: String, locations: Seq[Location])
    case class AddLocationsFailed(db: String, namespace: String, locations: Seq[Location])
    case class LocationDeleted(db: String, namespace: String, location: Location)
    case class NamespaceDeleted(db: String, namespace: String, occurredOn: Long)

    case class MetricInfoGot(db: String, namespace: String, metricInfo: Option[MetricInfo])
    case class MetricInfoPut(db: String, namespace: String, metricInfo: MetricInfo)
    case class MetricInfoFailed(db: String, namespace: String, metricInfo: MetricInfo, message: String)
  }

  def props(cache: ActorRef, mediator: ActorRef): Props = Props(new MetadataCoordinator(cache, mediator))
}
