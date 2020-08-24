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

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.ddata._
import akka.dispatch.ControlMessage
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.logic.WriteConfig
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.{Coordinates, NSDbSerializable}
import io.radicalbit.nsdb.model.{Location, LocationWithCoordinates}
import io.radicalbit.nsdb.util.ErrorManagementUtils

import scala.concurrent.Future

object ReplicatedMetadataCache {

  /**
    * Cache key for a metric locations.
    * @param db metric db.
    * @param namespace metric name.
    * @param metric metric name.
    */
  case class MetricLocationsCacheKey(db: String, namespace: String, metric: String) extends NSDbSerializable

  /**
    * Cache key for the metric info.
    * @param db metric db.
    * @param namespace metric name.
    * @param metric metric name.
    */
  case class MetricInfoCacheKey(db: String, namespace: String, metric: String) extends NSDbSerializable

  private final case class MetricLocationsRequest(key: MetricLocationsCacheKey, replyTo: ActorRef)
  private final case class MetricLocationsInNodeRequest(db: String,
                                                        namespace: String,
                                                        metric: String,
                                                        node: String,
                                                        replyTo: ActorRef)
  private final case class MetricInfoRequest(key: MetricInfoCacheKey, replyTo: ActorRef)
  private final case class AllMetricInfoWithRetentionRequest(replyTo: ActorRef)
  private final case class AllMetricInfoRequest(replyTo: ActorRef)
  private final case class DbsRequest(replyTo: ActorRef)
  private final case class NamespaceRequest(db: String, replyTo: ActorRef)
  private final case class MetricRequest(db: String, namespace: String, replyTo: ActorRef)

  final case class PutCoordinateInCache(db: String, namespace: String, metric: String) extends NSDbSerializable
  final case class PutLocationInCache(db: String, namespace: String, metric: String, value: Location)
      extends NSDbSerializable
  final case class GetLocationsFromCache(db: String, namespace: String, metric: String) extends NSDbSerializable
  final case class GetLocationsInNodeFromCache(db: String, namespace: String, metric: String, nodeName: String)
      extends NSDbSerializable
  final case object GetDbsFromCache                                   extends NSDbSerializable
  final case class GetNamespacesFromCache(db: String)                 extends NSDbSerializable
  final case class GetMetricsFromCache(db: String, namespace: String) extends NSDbSerializable

  sealed trait AddCoordinateResponse                                               extends NSDbSerializable
  final case class CoordinateCached(db: String, namespace: String, metric: String) extends AddCoordinateResponse
  final case class PutCoordinateInCacheFailed(db: String, namespace: String, metric: String)
      extends AddCoordinateResponse

  sealed trait AddLocationResponse

  final case class LocationCached(db: String, namespace: String, metric: String, value: Location)
      extends AddLocationResponse
  final case class LocationFromCacheGot(db: String,
                                        namespace: String,
                                        metric: String,
                                        from: Long,
                                        to: Long,
                                        value: Option[Location])
  final case class PutLocationInCacheFailed(db: String, namespace: String, metric: String, location: Location)
      extends AddLocationResponse

  final case class LocationsCached(db: String, namespace: String, metric: String, locations: Seq[Location])
      extends NSDbSerializable
  final case class EvictLocation(db: String, namespace: String, location: Location) extends NSDbSerializable
  final case class EvictLocationsInNode(nodeName: String)                           extends NSDbSerializable

  final case class LocationEvicted(db: String, namespace: String, location: Location)     extends NSDbSerializable
  final case class EvictLocationFailed(db: String, namespace: String, location: Location) extends NSDbSerializable
  final case class LocationsInNodeEvicted(nodeName: String)                               extends NSDbSerializable
  final case class EvictLocationsInNodeFailed(nodeName: String)                           extends NSDbSerializable

  final case class PutMetricInfoInCache(metricInfo: MetricInfo)                          extends NSDbSerializable
  final case class MetricInfoAlreadyExisting(key: MetricInfoCacheKey, value: MetricInfo) extends NSDbSerializable

  final case class MetricInfoCached(db: String, namespace: String, metric: String, value: Option[MetricInfo])
      extends NSDbSerializable
  final case class AllMetricInfoGot(metricInfo: Set[MetricInfo]) extends ControlMessage with NSDbSerializable
  final case class AllMetricInfoWithRetentionGot(metricInfo: Set[MetricInfo])
      extends ControlMessage
      with NSDbSerializable

  final case class GetMetricInfoFromCache(db: String, namespace: String, metric: String) extends NSDbSerializable
  final case object GetAllMetricInfoWithRetention                                        extends NSDbSerializable

  final case class CacheError(error: String)

  final case class DbsFromCacheGot(dbs: Set[String])                                        extends NSDbSerializable
  final case class NamespacesFromCacheGot(db: String, namespaces: Set[String])              extends NSDbSerializable
  final case class MetricsFromCacheGot(db: String, namespace: String, metrics: Set[String]) extends NSDbSerializable

  final case class DropMetricFromCache(db: String, namespace: String, metric: String) extends NSDbSerializable
  final case class DropNamespaceFromCache(db: String, namespace: String)              extends NSDbSerializable

  final case class MetricFromCacheDropped(db: String, namespace: String, metric: String)    extends NSDbSerializable
  final case class DropMetricFromCacheFailed(db: String, namespace: String, metric: String) extends NSDbSerializable
  final case class NamespaceFromCacheDropped(db: String, namespace: String)                 extends NSDbSerializable
  final case class DropNamespaceFromCacheFailed(db: String, namespace: String)              extends NSDbSerializable
}

/**
  * cluster aware cache to store metric's locations based on [[akka.cluster.ddata.Replicator]]
  */
class ReplicatedMetadataCache extends Actor with ActorLogging with WriteConfig {

  import ReplicatedMetadataCache._
  import akka.cluster.ddata.Replicator._

  val replicator: ActorRef = DistributedData(context.system).replicator

  implicit val address: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

  /**
    * convert a [[MetricLocationsCacheKey]] into an internal cache key
    *
    * @param metricKey the metric key to convert
    * @return [[ORSetKey]] resulted from metricKey hashCode
    */
  private def metricLocationsKey(metricKey: MetricLocationsCacheKey): ORSetKey[Location] =
    ORSetKey(s"metric-locations-cache-${metricKey.db}-${metricKey.namespace}-${metricKey.metric}")

  /**
    * convert a node name into an internal cache key
    *
    * @param nodeName the node name to convert
    * @return [[ORSetKey]] resulted from metricKey hashCode
    */
  private def nodeLocationsKey(nodeName: String): ORSetKey[LocationWithCoordinates] =
    ORSetKey(s"node-locations-cache-$nodeName")

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

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  def receive: Receive = {
    case PutCoordinateInCache(db, namespace, metric) =>
      (replicator ? Update(coordinatesKey, ORSet(), metadataWriteConsistency)(_ :+ Coordinates(db, namespace, metric)))
        .map {
          case UpdateSuccess(_, _) =>
            CoordinateCached(db, namespace, metric)
          case e =>
            log.error(s"error in put coordinate in cache $e")
            PutCoordinateInCacheFailed(db, namespace, metric)
        }
    case PutLocationInCache(db, namespace, metric, location) =>
      val metricKey = MetricLocationsCacheKey(db, namespace, metric)
      (for {
        loc <- (replicator ? Update(metricLocationsKey(metricKey), ORSet(), metadataWriteConsistency)(_ :+ location))
          .map {
            case UpdateSuccess(_, _) =>
              LocationCached(db, namespace, metric, location)
            case e =>
              log.error(s"error in put location in cache $e")
              PutLocationInCacheFailed(db, namespace, metric, location)
          }
        _ <- replicator ? Update(nodeLocationsKey(location.node), ORSet(), metadataWriteConsistency)(
          _ :+ LocationWithCoordinates(db, namespace, location))
        _ <- replicator ? Update(coordinatesKey, ORSet(), metadataWriteConsistency)(
          _ :+ Coordinates(db, namespace, metric))
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
                  _ <- replicator ? Update(allMetricInfoKey, ORSet(), metadataWriteConsistency)(_ :+ metricInfo)
                  res <- replicator ? Update(metricInfoKey(key), LWWMap(), metadataWriteConsistency)(
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
              _ <- replicator ? Update(allMetricInfoKey, ORSet(), metadataWriteConsistency)(_ :+ metricInfo)
              res <- replicator ? Update(metricInfoKey(key), LWWMap(), metadataWriteConsistency)(
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
      val metricKey = MetricLocationsCacheKey(db, namespace, metric)
      val f = for {
        remove <- replicator ? Update(metricLocationsKey(metricKey), ORSet(), metadataWriteConsistency)(_ remove loc)
        _ <- replicator ? Update(nodeLocationsKey(node), ORSet(), metadataWriteConsistency)(
          _ remove LocationWithCoordinates(db, namespace, loc))
      } yield remove
      f.map {
          case UpdateSuccess(_, _) =>
            Right(LocationEvicted(db, namespace, Location(metric, node, from, to)))
          case e =>
            log.error(s"unexpected result during location eviction $e")
            Left(EvictLocationFailed(db, namespace, Location(metric, node, from, to)))
        }
        .pipeTo(sender)
    case EvictLocationsInNode(nodeName) =>
      (replicator ? Get(nodeLocationsKey(nodeName), ReadLocal))
        .flatMap {
          case g @ GetSuccess(ORSetKey(_), _) =>
            Future
              .sequence(
                g.dataValue
                  .asInstanceOf[ORSet[LocationWithCoordinates]]
                  .elements
                  .map {
                    case LocationWithCoordinates(db, namespace, location) =>
                      (self ? EvictLocation(db, namespace, location))
                        .mapTo[Either[EvictLocationFailed, LocationEvicted]]
                  }
              )
              .map { responses =>
                val (_, failures) = ErrorManagementUtils.partitionResponses(responses)
                if (failures.nonEmpty) {
                  log.error(s"error in evicting locations $failures")
                  Left(EvictLocationsInNodeFailed(nodeName))
                } else Right(LocationsInNodeEvicted(nodeName))
              }
          case NotFound(_, _) =>
            Future(Right(LocationsInNodeEvicted(nodeName)))
          case e =>
            log.error(s"cannot retrieve locations for node $nodeName, got $e")
            Future(Left(EvictLocationsInNodeFailed(nodeName)))
        }
        .pipeTo(sender)
    case GetLocationsFromCache(db, namespace, metric) =>
      val key = MetricLocationsCacheKey(db, namespace, metric)
      log.debug("searching for key {} in cache", key)
      replicator ! Get(metricLocationsKey(key), ReadLocal, Some(MetricLocationsRequest(key, sender())))
    case GetLocationsInNodeFromCache(db, namespace, metric, nodeName) =>
      val key = nodeLocationsKey(nodeName)
      log.debug("searching for key {} in cache", key)
      replicator ! Get(key, ReadLocal, Some(MetricLocationsInNodeRequest(db, namespace, metric, nodeName, sender())))
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
        locations <- (self ? GetLocationsFromCache(db, namespace, metric)).mapTo[LocationsCached].map(_.locations)
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
                (self ? GetLocationsFromCache(db, namespace, metric)).mapTo[LocationsCached].map(_.locations))
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
    case g @ GetSuccess(ORSetKey(_), Some(MetricLocationsInNodeRequest(db, namespace, metric, nodeName, replyTo))) =>
      val values = g.dataValue
        .asInstanceOf[ORSet[LocationWithCoordinates]]
        .elements
        .collect {
          case LocationWithCoordinates(d, n, location) if d == db && n == namespace && location.metric == metric =>
            location
        }
        .toList
      replyTo ! LocationsCached(db, namespace, metric, values)
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
    case NotFound(_, Some(MetricLocationsInNodeRequest(db, namespace, metric, nodeName, replyTo))) =>
      replyTo ! LocationsCached(db, namespace, metric, Seq.empty)
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
