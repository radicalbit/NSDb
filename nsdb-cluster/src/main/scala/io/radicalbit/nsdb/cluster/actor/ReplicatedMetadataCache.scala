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

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ddata._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.index.{Location, MetricInfo}

import scala.concurrent.Future
import scala.concurrent.duration._

object ReplicatedMetadataCache {

  /**
    * Cache key for a shard location
    * @param db location db.
    * @param namespace location namespace.
    * @param metric location metric.
    * @param from location lower bound.
    * @param to location upperbound.
    */
  case class LocationKey(db: String, namespace: String, metric: String, from: Long, to: Long)

  /**
    * Cache key for a metric locations.
    * @param db metric db.
    * @param namespace metric name.
    * @param metric metric name.
    */
  case class MetricLocationsKey(db: String, namespace: String, metric: String)

  /**
    * Cache key for the metric info
    * @param db metric db.
    * @param namespace metric name.
    * @param metric metric name.
    */
  case class MetricInfoKey(db: String, namespace: String, metric: String)

  private final case class SingleLocationRequest(key: LocationKey, replyTo: ActorRef)
  private final case class MetricLocationsRequest(key: MetricLocationsKey, replyTo: ActorRef)
  private final case class MetricInfoRequest(key: MetricInfoKey, replyTo: ActorRef)

  final case class PutLocationInCache(db: String,
                                      namespace: String,
                                      metric: String,
                                      from: Long,
                                      to: Long,
                                      value: Location)
  final case class GetLocationFromCache(db: String, namespace: String, metric: String, from: Long, to: Long)
  final case class GetLocationsFromCache(db: String, namespace: String, metric: String)
  final case class LocationCached(db: String,
                                  namespace: String,
                                  metric: String,
                                  from: Long,
                                  to: Long,
                                  value: Option[Location])
  final case class LocationsCached(db: String, namespace: String, metric: String, value: Seq[Location])
  final case class EvictLocation(db: String, namespace: String, metric: String, from: Long, to: Long)

  final case class PutMetricInfoInCache(db: String, namespace: String, metric: String, value: MetricInfo)
  final case class MetricInfoAlreadyExisting(key: MetricInfoKey, value: MetricInfo)

  final case class MetricInfoCached(db: String, namespace: String, metric: String, value: Option[MetricInfo])

  final case class GetMetricInfoFromCache(db: String, namespace: String, metric: String)

  final case class CacheError(error: String)

}

/**
  * cluster aware cache to store metric's locations based on [[akka.cluster.ddata.Replicator]]
  */
class ReplicatedMetadataCache extends Actor with ActorLogging {

  import ReplicatedMetadataCache._
  import akka.cluster.ddata.Replicator._

  implicit val cluster: Cluster = Cluster(context.system)

  val replicator: ActorRef = DistributedData(context.system).replicator

  /**
    * convert a [[LocationKey]] into an internal cache key
    * @param locKey the location key to convert
    * @return [[LWWMapKey]] resulted from locKey hashCode
    */
  private def locationKey(locKey: LocationKey): LWWMapKey[LocationKey, Location] =
    LWWMapKey("location-cache-" + math.abs(locKey.hashCode) % 100)

  /**
    * convert a [[MetricLocationsKey]] into an internal cache key
    *
    * @param metricKey the metric key to convert
    * @return [[LWWMapKey]] resulted from metricKey hashCode
    */
  private def metricLocationsKey(metricKey: MetricLocationsKey): LWWMapKey[MetricLocationsKey, Seq[Location]] =
    LWWMapKey("metric-locations-cache-" + math.abs(metricKey.hashCode) % 100)

  /**
    * convert a [[MetricInfoKey]] into an internal cache key
    *
    * @param metricInfoKey the metric info key to convert
    * @return [[LWWMapKey]] resulted from metricKey hashCode
    */
  private def metricInfoKey(metricInfoKey: MetricInfoKey): LWWMapKey[MetricInfoKey, MetricInfo] =
    LWWMapKey("metric-info-cache-" + math.abs(metricInfoKey.hashCode) % 100)

  private val writeDuration = 5.seconds

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  def receive: Receive = {
    case PutLocationInCache(db, namespace, metric, from, to, value) =>
      val key = LocationKey(db, namespace, metric, from, to)
      val f = for {
        loc <- (replicator ? Update(locationKey(key), LWWMap(), WriteAll(writeDuration))(_ + (key -> value)))
          .map {
            case UpdateSuccess(_, _) =>
              LocationCached(db, namespace, metric, from, to, Some(value))
            case _ => LocationCached(db, namespace, metric, from, to, None)
          }
        metricKey = MetricLocationsKey(key.db, key.namespace, key.metric)
        getMetric <- (replicator ? Get(metricLocationsKey(metricKey), ReadLocal)).map {
          case g @ GetSuccess(LWWMapKey(_), _) =>
            LocationsCached(
              key.db,
              key.namespace,
              key.metric,
              g.dataValue
                .asInstanceOf[LWWMap[MetricLocationsKey, Seq[Location]]]
                .get(metricKey)
                .getOrElse(Seq.empty)
            )
          case _ =>
            LocationsCached(db, namespace, metric, Seq.empty)
        }
        _ <- {
          val locMap  = getMetric.value.map(e => (e.from, e.to) -> e).toMap
          val newLocs = (locMap + ((value.from, value.to) -> value)).values.map(identity).toSeq
          (replicator ? Update(metricLocationsKey(metricKey), LWWMap(), WriteAll(writeDuration))(
            _ + (metricKey -> newLocs)))
            .map {
              case UpdateSuccess(_, _) =>
                LocationsCached(db, namespace, metric, newLocs)
              case _ => LocationCached(db, namespace, metric, from, to, None)
            }
        }
      } yield loc
      f.pipeTo(sender())
    case PutMetricInfoInCache(db, namespace, metric, value) =>
      val key = MetricInfoKey(db, namespace, metric)
      (replicator ? Get(metricInfoKey(key), ReadLocal))
        .flatMap {
          case g @ GetSuccess(LWWMapKey(_), _) =>
            g.dataValue
              .asInstanceOf[LWWMap[MetricInfoKey, MetricInfo]]
              .get(key) match {
              case Some(metricInfo) => Future(MetricInfoAlreadyExisting(key, metricInfo))
              case None =>
                (replicator ? Update(metricInfoKey(key), LWWMap(), WriteAll(writeDuration))(_ + (key -> value)))
                  .map {
                    case UpdateSuccess(_, _) =>
                      MetricInfoCached(db, namespace, metric, Some(value))
                    case _ => MetricInfoCached(db, namespace, metric, None)
                  }
            }
          case NotFound(_, _) =>
            (replicator ? Update(metricInfoKey(key), LWWMap(), WriteAll(writeDuration))(_ + (key -> value)))
              .map {
                case UpdateSuccess(_, _) =>
                  MetricInfoCached(db, namespace, metric, Some(value))
                case _ => MetricInfoCached(db, namespace, metric, None)
              }
          case e => Future(CacheError(s"cannot retrieve metric info for key $key, got $e"))
        }
        .pipeTo(sender)

    case EvictLocation(db, namespace, metric, from, to) =>
      val key = LocationKey(db, namespace, metric, from, to)
      val f = for {
        loc <- (replicator ? Update(locationKey(key), LWWMap(), WriteAll(writeDuration))(_ - key))
          .map(_ => LocationCached(db, namespace, metric, from, to, None))
        metricKey = MetricLocationsKey(key.db, key.namespace, key.metric)
        getMetric <- (replicator ? Get(metricLocationsKey(metricKey), ReadLocal)).map {
          case g @ GetSuccess(LWWMapKey(_), _) =>
            LocationsCached(
              db,
              namespace,
              metric,
              g.dataValue
                .asInstanceOf[LWWMap[MetricLocationsKey, Seq[Location]]]
                .get(metricKey)
                .getOrElse(Seq.empty)
            )
          case _ =>
            LocationsCached(db, namespace, metric, Seq.empty)
        }
        _ <- {
          val locMap  = getMetric.value.map(e => (e.from, e.to) -> e).toMap
          val newLocs = (locMap - ((key.from, key.to))).values.map(identity).toSeq
          (replicator ? Update(metricLocationsKey(metricKey), LWWMap(), WriteAll(writeDuration))(
            _ + (metricKey -> newLocs)))
            .map {
              case UpdateSuccess(_, _) =>
                LocationsCached(db, namespace, metric, newLocs)
              case _ => LocationCached(db, namespace, metric, from, to, None)
            }
        }
      } yield loc
      f.pipeTo(sender)
    case GetLocationFromCache(db, namespace, metric, from, to) =>
      val key = LocationKey(db, namespace, metric, from, to)
      log.debug("searching for key {} in cache", key)
      replicator ! Get(locationKey(key), ReadLocal, Some(SingleLocationRequest(key, sender())))
    case GetLocationsFromCache(db, namespace, metric) =>
      val key = MetricLocationsKey(db, namespace, metric)
      log.debug("searching for key {} in cache", key)
      replicator ! Get(metricLocationsKey(key), ReadLocal, Some(MetricLocationsRequest(key, sender())))
    case GetMetricInfoFromCache(db, namespace, metric) =>
      val key = MetricInfoKey(db, namespace, metric)
      log.debug("searching for key {} in cache", key)
      replicator ! Get(metricInfoKey(key), ReadLocal, Some(MetricInfoRequest(key, sender())))
    case g @ GetSuccess(LWWMapKey(_), Some(SingleLocationRequest(key, replyTo))) =>
      g.dataValue.asInstanceOf[LWWMap[LocationKey, Location]].get(key) match {
        case Some(value) => replyTo ! LocationCached(key.db, key.namespace, key.metric, key.from, key.to, Some(value))
        case None        => replyTo ! LocationCached(key.db, key.namespace, key.metric, key.from, key.to, None)
      }
    case g: GetFailure[_] =>
      log.error(s"Error in cache GetFailure: $g")
    case g @ GetSuccess(LWWMapKey(_), Some(MetricLocationsRequest(key, replyTo))) =>
      g.dataValue
        .asInstanceOf[LWWMap[MetricLocationsKey, Seq[Location]]]
        .get(MetricLocationsKey(key.db, key.namespace, key.metric)) match {
        case Some(value) => replyTo ! LocationsCached(key.db, key.namespace, key.metric, value)
        case None        => replyTo ! LocationsCached(key.db, key.namespace, key.metric, Seq.empty)
      }
    case g @ GetSuccess(LWWMapKey(_), Some(MetricInfoRequest(key, replyTo))) =>
      val dataValue = g.dataValue
        .asInstanceOf[LWWMap[MetricInfoKey, MetricInfo]]
        .get(key)
      replyTo ! MetricInfoCached(key.db, key.namespace, key.metric, dataValue)
    case NotFound(_, Some(SingleLocationRequest(key, replyTo))) =>
      replyTo ! LocationCached(key.db, key.namespace, key.metric, key.from, key.to, None)
    case NotFound(_, Some(MetricLocationsRequest(key, replyTo))) =>
      replyTo ! LocationsCached(key.db, key.namespace, key.metric, Seq.empty)
    case NotFound(_, Some(MetricInfoRequest(key, replyTo))) =>
      replyTo ! MetricInfoCached(key.db, key.namespace, key.metric, None)
    case msg: UpdateResponse[_] =>
      log.debug("received not handled update message {}", msg)
  }
}
