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
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.pattern._
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.MetadataActor.MetricMetadata
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.index.{Location, MetricInfo}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.WarmUpCompleted
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.concurrent.{Await, Future}

/**
  * Actor that handles metadata (i.e. write location for metrics)
  * @param cache cluster aware metric's location cache
  */
class MetadataCoordinator(cache: ActorRef, mediator: ActorRef) extends ActorPathLogging with Stash {

  log.error(s"MetadataCoordinator path: ${self.path}")

  val cluster = Cluster(context.system)

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.metadata-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher
  import scala.concurrent.duration._

  lazy val defaultShardingInterval: Long =
    context.system.settings.config.getDuration("nsdb.sharding.interval").toMillis

  lazy val warmUpTopic   = context.system.settings.config.getString("nsdb.cluster.pub-sub.warm-up-topic")
  lazy val metadataTopic = context.system.settings.config.getString("nsdb.cluster.pub-sub.metadata-topic")

  override def receive: Receive = warmUp

  def warmUp: Receive = {
    case msg @ WarmUpMetadata(metricsMetadata) =>
      log.info(s"Received location warm-up message: $msg ")
      Future
        .sequence(metricsMetadata.map { metadata =>
          Future
            .sequence(metadata.locations.map { location =>
              (cache ? PutLocationInCache(
                LocationKey(metadata.db, metadata.namespace, metadata.metric, location.from, location.to),
                location))
                .mapTo[LocationCached]
            })
            .flatMap { _ =>
              metadata.info.map(metricInfo =>
                (cache ? PutMetricInfoInCache(MetricInfoKey(metadata.db, metadata.namespace, metricInfo.metric),
                                              metricInfo))
                  .mapTo[MetricInfoCached]) getOrElse Future(MetricInfoGot(metadata.db, metadata.namespace, None))
            }
        })
        .recover {
          case t =>
            log.error(t, "error during warm up")
            context.system.terminate()
        }
        .foreach { _ =>
          mediator ! Publish(warmUpTopic, WarmUpCompleted)
          unstashAll()
          context.become(operative)
        }

    case msg =>
      stash()
      log.error(s"Received and stashed message $msg during warmUp")
  }

  private def getShardStartIstant(timestamp: Long, shardInterval: Long) = (timestamp / shardInterval) * shardInterval

  private def getShardEndIstant(startShard: Long, shardInterval: Long) = startShard + shardInterval

  private def performAddIntoCache(db: String, namespace: String, location: Location) = {
    (cache ? PutLocationInCache(LocationKey(db, namespace, location.metric, location.from, location.to), location))
      .map {
        case LocationCached(_, Some(_)) =>
          mediator ! Publish(metadataTopic, AddLocation(db, namespace, location))
          LocationAdded(db, namespace, location)
        case _ => AddLocationFailed(db, namespace, location)
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
    (cache ? GetMetricInfoFromCache(MetricInfoKey(db, namespace, metric)))
      .flatMap {
        case MetricInfoCached(_, Some(metricInfo)) => Future(metricInfo.shardInterval)
        case _ =>
          (cache ? PutMetricInfoInCache(MetricInfoKey(db, namespace, metric),
                                        MetricInfo(metric, defaultShardingInterval)))
            .map(_ => defaultShardingInterval)
      }

  /**
    * behaviour in case shard is true
    */
  def operative: Receive = {
    case GetLocations(db, namespace, metric) =>
      val f = (cache ? GetLocationsFromCache(MetricLocationsKey(db, namespace, metric)))
        .mapTo[LocationsCached]
        .map(l => LocationsGot(db, namespace, metric, l.value))
      f.pipeTo(sender())
    case GetWriteLocation(db, namespace, metric, timestamp) =>
      val nodeName =
        s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"
      val replyTo = sender()

      // FIXME this operation must be blocking otherwise we may have concurrent location update, a solution could be delegation
      // There must be a pool of actor one for each metric
      val reply = (cache ? GetLocationsFromCache(MetricLocationsKey(db, namespace, metric)))
        .flatMap {
          case LocationsCached(_, values) if values.nonEmpty =>
            values.find(v => v.from <= timestamp && v.to >= timestamp) match {
              case Some(loc) => Future(LocationGot(db, namespace, metric, Some(loc)))
              case None =>
                getShardInterval(db, namespace, metric)
                  .flatMap { interval =>
                    val start = getShardStartIstant(timestamp, interval)
                    val end   = getShardEndIstant(start, interval)
                    performAddIntoCache(db, namespace, Location(metric, nodeName, start, end)).map {
                      case LocationAdded(_, _, location) => LocationGot(db, namespace, metric, Some(location))
                      case AddLocationFailed(_, _, _)    => LocationGot(db, namespace, metric, None)
                    }
                  }

            }
          case LocationsCached(_, _) =>
            getShardInterval(db, namespace, metric)
              .flatMap { interval =>
                val start = getShardStartIstant(timestamp, interval)
                val end   = getShardEndIstant(start, interval)
                performAddIntoCache(db, namespace, Location(metric, nodeName, start, end)).map {
                  case LocationAdded(_, _, location) => LocationGot(db, namespace, metric, Some(location))
                  case AddLocationFailed(_, _, _)    => LocationGot(db, namespace, metric, None)
                }
              }
        }

      val response = Await.result(reply, 1 second)
      replyTo ! response
    case msg @ AddLocation(db, namespace, location) =>
      performAddIntoCache(db, namespace, location).pipeTo(sender)
    case GetMetricInfo(db, namespace, metric) =>
      (cache ? GetMetricInfoFromCache(MetricInfoKey(db, namespace, metric)))
        .map {
          case MetricInfoCached(_, value) => MetricInfoGot(db, namespace, value)
        }
        .pipeTo(sender)
    case msg @ PutMetricInfo(db, namespace, metricInfo) =>
      (cache ? PutMetricInfoInCache(MetricInfoKey(db, namespace, metricInfo.metric), metricInfo))
        .map {
          case MetricInfoCached(_, Some(_)) =>
            mediator ! Publish(metadataTopic, msg)
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
    case class GetWriteLocation(db: String, namespace: String, metric: String, timestamp: Long)
    case class AddLocation(db: String, namespace: String, location: Location)
    case class AddLocations(db: String, namespace: String, locations: Seq[Location])
    case class DeleteLocation(db: String, namespace: String, location: Location)
    case class DeleteNamespace(db: String, namespace: String, occurredOn: Long = System.currentTimeMillis)

    case class GetMetricInfo(db: String, namespace: String, metric: String)
    case class PutMetricInfo(db: String, namespace: String, metricInfo: MetricInfo)
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

    case class MetricInfoGot(db: String, namespace: String, metricInfo: Option[MetricInfo])
    case class MetricInfoPut(db: String, namespace: String, metricInfo: MetricInfo)
    case class MetricInfoFailed(db: String, namespace: String, metricInfo: MetricInfo, message: String)
  }

  def props(cache: ActorRef, mediator: ActorRef): Props = Props(new MetadataCoordinator(cache, mediator))
}
