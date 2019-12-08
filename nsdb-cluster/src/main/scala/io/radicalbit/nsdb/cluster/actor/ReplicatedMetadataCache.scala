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
import akka.cluster.ddata._
import akka.dispatch.ControlMessage
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.util.ErrorManagementUtils
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.Coordinates
import io.radicalbit.nsdb.model.Location

import scala.concurrent.Future
import scala.concurrent.duration._

object ReplicatedMetadataCache {

  /**
    * internal key to store multiple location (for multiple nodes) per time interval.
    * @param db metric db.
    * @param namespace metric namespace.
    * @param metric metric name.
    * @param node location node.
    * @param from location lower bound.
    * @param to location upper bound.
    */
  case class LocationKey(db: String, namespace: String, metric: String, node: String, from: Long, to: Long)

  /**
    * Cache key for a metric locations.
    * @param db metric db.
    * @param namespace metric name.
    * @param metric metric name.
    */
  case class MetricLocationsCacheKey(db: String, namespace: String, metric: String)

  /**
    * Cache key for the metric info.
    * @param db metric db.
    * @param namespace metric name.
    * @param metric metric name.
    */
  case class MetricInfoCacheKey(db: String, namespace: String, metric: String)

  private final case class MetricLocationsRequest(key: MetricLocationsCacheKey, replyTo: ActorRef)
  private final case class MetricInfoRequest(key: MetricInfoCacheKey, replyTo: ActorRef)
  private final case class AllMetricInfoWithRetentionRequest(replyTo: ActorRef)
  private final case class AllMetricInfoRequest(replyTo: ActorRef)
  private final case class DbsRequest(replyTo: ActorRef)
  private final case class NamespaceRequest(db: String, replyTo: ActorRef)
  private final case class MetricRequest(db: String, namespace: String, replyTo: ActorRef)

  final case class PutLocationInCache(db: String,
                                      namespace: String,
                                      metric: String,
                                      from: Long,
                                      to: Long,
                                      value: Location)
  final case class GetLocationsFromCache(db: String, namespace: String, metric: String)
  final case object GetDbsFromCache
  final case class GetNamespacesFromCache(db: String)
  final case class GetMetricsFromCache(db: String, namespace: String)

  sealed trait AddLocationResponse

  final case class LocationCached(db: String, namespace: String, metric: String, from: Long, to: Long, value: Location)
      extends AddLocationResponse
  final case class LocationFromCacheGot(db: String,
                                        namespace: String,
                                        metric: String,
                                        from: Long,
                                        to: Long,
                                        value: Option[Location])
  final case class PutLocationInCacheFailed(db: String, namespace: String, metric: String, location: Location)
      extends AddLocationResponse

  final case class LocationsCached(db: String, namespace: String, metric: String, value: Seq[Location])
  final case class EvictLocation(db: String, namespace: String, location: Location)

  final case class LocationEvicted(db: String, namespace: String, location: Location)
  final case class EvictLocationFailed(db: String, namespace: String, location: Location)

  final case class PutMetricInfoInCache(metricInfo: MetricInfo)
  final case class MetricInfoAlreadyExisting(key: MetricInfoCacheKey, value: MetricInfo)

  final case class MetricInfoCached(db: String, namespace: String, metric: String, value: Option[MetricInfo])
  final case class AllMetricInfoGot(metricInfo: Set[MetricInfo])              extends ControlMessage
  final case class AllMetricInfoWithRetentionGot(metricInfo: Set[MetricInfo]) extends ControlMessage

  final case class GetMetricInfoFromCache(db: String, namespace: String, metric: String)
  final case object GetAllMetricInfoWithRetention

  final case class CacheError(error: String)

  final case class DbsFromCacheGot(dbs: Set[String])
  final case class NamespacesFromCacheGot(db: String, namespaces: Set[String])
  final case class MetricsFromCacheGot(db: String, namespace: String, metrics: Set[String])

  final case class DropMetricFromCache(db: String, namespace: String, metric: String)
  final case class DropNamespaceFromCache(db: String, namespace: String)

  final case class MetricFromCacheDropped(db: String, namespace: String, metric: String)
  final case class DropMetricFromCacheFailed(db: String, namespace: String, metric: String)
  final case class NamespaceFromCacheDropped(db: String, namespace: String)
  final case class DropNamespaceFromCacheFailed(db: String, namespace: String)
}

/**
  * cluster aware cache to store metric's locations based on [[akka.cluster.ddata.Replicator]]
  */
class ReplicatedMetadataCache extends Actor with ActorLogging {

  import ReplicatedMetadataCache._
  import akka.cluster.ddata.Replicator._

  val replicator: ActorRef = DistributedData(context.system).replicator

  implicit val address: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

  /**
    * convert a [[LocationKey]] into an internal cache key
    *
    * @param locKey the location key to convert
    * @return [[LWWMapKey]] resulted from locKey hashCode
    */
  private def locationKey(locKey: LocationKey): LWWMapKey[LocationKey, Location] =
    LWWMapKey("location-cache-" + math.abs(locKey.hashCode) % 100)

  /**
    * convert a [[MetricLocationsCacheKey]] into an internal cache key
    *
    * @param metricKey the metric key to convert
    * @return [[ORSetKey]] resulted from metricKey hashCode
    */
  private def metricLocationsKey(metricKey: MetricLocationsCacheKey): ORSetKey[Location] =
    ORSetKey(s"metric-locations-cache-${metricKey.db}-${metricKey.namespace}-${metricKey.metric}")

  /**
    * convert a [[MetricInfoCacheKey]] into an internal cache key
    *
    * @param metricInfoKey the metric info key to convert
    * @return [[LWWMapKey]] resulted from metricKey hashCode
    */
  private def metricInfoKey(metricInfoKey: MetricInfoCacheKey): LWWMapKey[MetricInfoCacheKey, MetricInfo] =
    LWWMapKey("metric-info-cache-" + math.abs(metricInfoKey.hashCode) % 100)

  /**
    * Creates a [[ORSetKey]] for all metric infos
    */
  private val allMetricInfoKey: ORSetKey[MetricInfo] = ORSetKey("all-metric-info-cache")

  /**
    * Creates a [[ORSetKey]] for databases
    */
  private val coordinatesKey: ORSetKey[Coordinates] = ORSetKey("coordinates-cache")

  private val writeDuration = 5.seconds

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  def receive: Receive = {
    case PutLocationInCache(db, namespace, metric, from, to, value) =>
      val key       = LocationKey(db, namespace, metric, value.node, from, to)
      val metricKey = MetricLocationsCacheKey(key.db, key.namespace, key.metric)
      (for {
        loc <- (replicator ? Update(locationKey(key), LWWMap(), WriteAll(writeDuration))(_ :+ (key -> value)))
          .map {
            case UpdateSuccess(_, _) =>
              LocationCached(db, namespace, metric, from, to, value)
            case e =>
              log.error(s"error in put location in cache $e")
              PutLocationInCacheFailed(db, namespace, metric, value)
          }
        _ <- replicator ? Update(metricLocationsKey(metricKey), ORSet(), WriteAll(writeDuration))(_ :+ value)
        _ <- replicator ? Update(coordinatesKey, ORSet(), WriteLocal)(_ :+ Coordinates(db, namespace, metric))
      } yield loc).pipeTo(sender())
    case PutMetricInfoInCache(metricInfo @ MetricInfo(db, namespace, metric, _, _)) =>
      val key = MetricInfoCacheKey(db, namespace, metric)
      (replicator ? Get(metricInfoKey(key), ReadLocal))
        .flatMap {
          case g @ GetSuccess(LWWMapKey(_), _) =>
            g.dataValue
              .asInstanceOf[LWWMap[MetricInfoCacheKey, MetricInfo]]
              .get(key) match {
              case Some(metricInfo) =>
                Future(MetricInfoAlreadyExisting(key, metricInfo))
              case None =>
                (for {
                  _ <- replicator ? Update(allMetricInfoKey, ORSet(), WriteAll(writeDuration))(_ :+ metricInfo)
                  res <- replicator ? Update(metricInfoKey(key), LWWMap(), WriteAll(writeDuration))(
                    _ :+ (key -> metricInfo))
                } yield res)
                  .map {
                    case UpdateSuccess(_, _) =>
                      MetricInfoCached(db, namespace, metric, Some(metricInfo))
                    case _ => MetricInfoCached(db, namespace, metric, None)
                  }
            }
          case NotFound(_, _) =>
            (for {
              _ <- replicator ? Update(allMetricInfoKey, ORSet(), WriteAll(writeDuration))(_ :+ metricInfo)
              res <- replicator ? Update(metricInfoKey(key), LWWMap(), WriteAll(writeDuration))(
                _ :+ (key -> metricInfo))
            } yield res)
              .map {
                case UpdateSuccess(_, _) =>
                  MetricInfoCached(db, namespace, metric, Some(metricInfo))
                case _ => MetricInfoCached(db, namespace, metric, None)
              }
          case e => Future(CacheError(s"cannot retrieve metric info for key $key, got $e"))
        }
        .pipeTo(sender)

    case EvictLocation(db, namespace, loc @ Location(metric, node, from, to)) =>
      val key       = LocationKey(db, namespace, metric, node, from, to)
      val metricKey = MetricLocationsCacheKey(key.db, key.namespace, key.metric)
      val f = for {
        _ <- replicator ? Update(locationKey(key), LWWMap(), WriteMajority(writeDuration))(_ remove (address, key))
        remove <- replicator ? Update(metricLocationsKey(metricKey), ORSet(), WriteMajority(writeDuration))(
          _ remove loc)
      } yield remove
      f.map {
          case UpdateSuccess(_, _) =>
            Right(LocationEvicted(db, namespace, Location(metric, node, from, to)))
          case _ => Left(EvictLocationFailed(db, namespace, Location(metric, node, from, to)))
        }
        .pipeTo(sender)
    case GetLocationsFromCache(db, namespace, metric) =>
      val key = MetricLocationsCacheKey(db, namespace, metric)
      log.debug("searching for key {} in cache", key)
      replicator ! Get(metricLocationsKey(key), ReadLocal, Some(MetricLocationsRequest(key, sender())))
    case GetAllMetricInfoWithRetention =>
      log.debug("searching for key {} in cache", allMetricInfoKey)
      replicator ! Get(allMetricInfoKey, ReadLocal, request = Some(AllMetricInfoWithRetentionRequest(sender())))
    case GetMetricInfoFromCache(db, namespace, metric) =>
      val key = MetricInfoCacheKey(db, namespace, metric)
      log.debug("searching for key {} in cache", key)
      replicator ! Get(metricInfoKey(key), ReadLocal, Some(MetricInfoRequest(key, sender())))
    case GetDbsFromCache =>
      log.debug("searching for key {} in cache", coordinatesKey)
      replicator ! Get(coordinatesKey, ReadLocal, request = Some(DbsRequest(sender())))
    case GetNamespacesFromCache(db) =>
      log.debug("searching for key {} in cache", coordinatesKey)
      replicator ! Get(coordinatesKey, ReadLocal, request = Some(NamespaceRequest(db, sender())))
    case GetMetricsFromCache(db, namespace) =>
      log.debug("searching for key {} in cache", coordinatesKey)
      replicator ! Get(coordinatesKey, ReadLocal, request = Some(MetricRequest(db, namespace, sender())))
    case DropMetricFromCache(db, namespace, metric) =>
      (for {
        locations <- (self ? GetLocationsFromCache(db, namespace, metric)).mapTo[LocationsCached].map(_.value)
        dropLocationsResult <- Future.sequence {
          locations.map(location => self ? EvictLocation(db, namespace, location))
        }
        metricInfo <- (self ? GetMetricInfoFromCache(db, namespace, metric)).mapTo[MetricInfoCached]
        _ <- metricInfo.value
          .map(info =>
            (replicator ? Update(allMetricInfoKey, ORSet(), WriteLocal)(_ remove info)).mapTo[UpdateSuccess[_]])
          .getOrElse(Future(UpdateSuccess(allMetricInfoKey, None)))
        _ <- {
          val key = MetricInfoCacheKey(db, namespace, metric)
          metricInfo.value
            .map(_ =>
              (replicator ? Update(metricInfoKey(key), LWWMap(), WriteLocal)(_ remove (address, key)))
                .mapTo[UpdateSuccess[_]])
            .getOrElse(Future(UpdateSuccess(metricInfoKey(key), None)))
        }
        dropCoordinates <- {
          val dropLocationsErrors = dropLocationsResult.collect { case res: EvictLocationFailed => res }
          if (dropLocationsErrors.isEmpty)
            (replicator ? Update(coordinatesKey, ORSet(), WriteLocal)(_ remove Coordinates(db, namespace, metric)))
              .map {
                case UpdateSuccess(_, _) =>
                  MetricFromCacheDropped(db, namespace, metric)
                case _ => DropMetricFromCacheFailed(db, namespace, metric)
              } else Future(DropMetricFromCacheFailed(db, namespace, metric))
        }
      } yield dropCoordinates)
        .pipeTo(sender())
    case DropNamespaceFromCache(db, namespace) =>
      val chain = for {
        metrics <- (self ? GetMetricsFromCache(db, namespace)).mapTo[MetricsFromCacheGot]
        locations <- {
          Future
            .sequence {
              metrics.metrics.map(metric =>
                (self ? GetLocationsFromCache(db, namespace, metric)).mapTo[LocationsCached].map(_.value))
            }
            .map(_.flatten)
        }
        (_, failedEvictions) <- Future
          .sequence {
            locations.map(location =>
              (self ? EvictLocation(db, namespace, location)).mapTo[Either[EvictLocationFailed, LocationEvicted]])
          }
          .map(responses => ErrorManagementUtils.partitionResponses(responses))
        metricInfos <- (replicator ? Get(allMetricInfoKey, ReadLocal, request = Some(AllMetricInfoRequest(sender()))))
          .map {
            case g @ GetSuccess(_, _) =>
              g.dataValue.asInstanceOf[ORSet[MetricInfo]].elements.filter(e => e.db == db && e.namespace == namespace)
            case NotFound(_, _) => Set.empty[MetricInfo]
          }
        _ <- Future.sequence(metricInfos.map(info =>
          (replicator ? Update(allMetricInfoKey, ORSet(), WriteLocal)(_ remove info)).mapTo[UpdateSuccess[_]]))
        _ <- Future.sequence(metricInfos.map { metricInfo =>
          val key = MetricInfoCacheKey(db, namespace, metricInfo.metric)
          (replicator ? Update(metricInfoKey(key), LWWMap(), WriteLocal)(_ remove (address, key)))
            .mapTo[UpdateSuccess[_]]
        })
        coordinatesUpdate <- if (failedEvictions.nonEmpty) {
          Future(DropNamespaceFromCacheFailed(db, namespace))
        } else
          Future
            .sequence(
              metrics.metrics
                .map(location =>
                  replicator ? Update(coordinatesKey, ORSet(), WriteLocal)(
                    _ remove Coordinates(db, namespace, location))))
            .map { responses =>
              val errors = responses.collect {
                case res: UpdateFailure[_] => res
              }
              if (errors.isEmpty) NamespaceFromCacheDropped(db, namespace)
              else DropNamespaceFromCacheFailed(db, namespace)
            }
      } yield coordinatesUpdate

      chain
        .recover {
          case _ => DropNamespaceFromCacheFailed(db, namespace)
        }
        .pipeTo(sender())
    case g: GetFailure[_] =>
      log.error(s"Error in cache GetFailure: $g")
    case g @ GetSuccess(ORSetKey(_), Some(MetricLocationsRequest(key, replyTo))) =>
      val values = g.dataValue
        .asInstanceOf[ORSet[Location]]
        .elements
        .toList
      replyTo ! LocationsCached(key.db, key.namespace, key.metric, values)
    case g @ GetSuccess(LWWMapKey(_), Some(MetricInfoRequest(key, replyTo))) =>
      val dataValue = g.dataValue
        .asInstanceOf[LWWMap[MetricInfoCacheKey, MetricInfo]]
        .get(key)
      replyTo ! MetricInfoCached(key.db, key.namespace, key.metric, dataValue)
    case g @ GetSuccess(_, Some(AllMetricInfoWithRetentionRequest(replyTo))) =>
      val elements = g.dataValue.asInstanceOf[ORSet[MetricInfo]].elements.filter(_.retention > 0)
      replyTo ! AllMetricInfoWithRetentionGot(elements)
    case g @ GetSuccess(_, Some(AllMetricInfoRequest(replyTo))) =>
      val elements = g.dataValue.asInstanceOf[ORSet[MetricInfo]].elements
      replyTo ! AllMetricInfoWithRetentionGot(elements)
    case g @ GetSuccess(_, Some(DbsRequest(replyTo))) =>
      val elements = g.dataValue.asInstanceOf[ORSet[Coordinates]].elements.map(_.db)
      replyTo ! DbsFromCacheGot(elements)
    case g @ GetSuccess(_, Some(NamespaceRequest(db, replyTo))) =>
      val elements = g.dataValue.asInstanceOf[ORSet[Coordinates]].elements.filter(_.db == db).map(_.namespace)
      replyTo ! NamespacesFromCacheGot(db, elements)
    case g @ GetSuccess(_, Some(MetricRequest(db, namespace, replyTo))) =>
      val elements = g.dataValue
        .asInstanceOf[ORSet[Coordinates]]
        .elements
        .filter(c => c.db == db && c.namespace == namespace)
        .map(_.metric)
      replyTo ! MetricsFromCacheGot(db, namespace, elements)
    case NotFound(_, Some(MetricLocationsRequest(key, replyTo))) =>
      replyTo ! LocationsCached(key.db, key.namespace, key.metric, Seq.empty)
    case NotFound(_, Some(MetricInfoRequest(key, replyTo))) =>
      replyTo ! MetricInfoCached(key.db, key.namespace, key.metric, None)
    case NotFound(_, Some(AllMetricInfoWithRetentionRequest(replyTo))) =>
      replyTo ! AllMetricInfoGot(Set.empty)
    case NotFound(_, Some(AllMetricInfoRequest(replyTo))) =>
      replyTo ! AllMetricInfoWithRetentionGot(Set.empty)
    case NotFound(_, Some(DbsRequest(replyTo))) =>
      replyTo ! DbsFromCacheGot(Set.empty)
    case NotFound(_, Some(NamespaceRequest(db, replyTo))) =>
      replyTo ! NamespacesFromCacheGot(db, Set.empty)
    case NotFound(_, Some(MetricRequest(db, namespace, replyTo))) =>
      replyTo ! MetricsFromCacheGot(db, namespace, Set.empty)
    case msg: UpdateResponse[_] =>
      log.debug("received not handled update message {}", msg)
  }
}
