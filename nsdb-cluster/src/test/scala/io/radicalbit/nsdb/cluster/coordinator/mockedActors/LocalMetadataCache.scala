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

package io.radicalbit.nsdb.cluster.coordinator.mockedActors

import java.util.concurrent.ConcurrentHashMap

import akka.actor.Actor
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.Coordinates
import io.radicalbit.nsdb.model.Location

import scala.collection.JavaConverters._
import scala.collection.mutable

class LocalMetadataCache extends Actor {

  import LocalMetadataCache.{DeleteAll, DeleteDone}

  val locations: mutable.Map[MetricLocationsCacheKey, Set[Location]] = mutable.Map.empty

  val metricInfo: ConcurrentHashMap[MetricInfoCacheKey, MetricInfo] = new ConcurrentHashMap

  val coordinates: mutable.Set[Coordinates] = mutable.Set.empty

  def receive: Receive = {
    case GetDbsFromCache =>
      sender ! DbsFromCacheGot(coordinates.map(_.db).toSet)
    case GetNamespacesFromCache(db) =>
      sender ! NamespacesFromCacheGot(db, coordinates.filter(c => c.db == db).map(_.namespace).toSet)
    case GetMetricsFromCache(db, namespace) =>
      sender ! MetricsFromCacheGot(db,
                                   namespace,
                                   coordinates.filter(c => c.db == db && c.namespace == namespace).map(_.metric).toSet)
    case DropMetricFromCache(db, namespace, metric) =>
      coordinates -= Coordinates(db, namespace, metric)
      locations --= locations.collect {
        case (key, _) if key.db == db && key.namespace == namespace && key.metric == metric => key
      }
      sender() ! MetricFromCacheDropped(db, namespace, metric)
    case DropNamespaceFromCache(db, namespace) =>
      coordinates --= coordinates.filter(c => c.db == db && c.namespace == namespace)
      sender() ! NamespaceFromCacheDropped(db, namespace)
    case PutLocationInCache(db, namespace, metric, value) =>
      val key              = MetricLocationsCacheKey(db, namespace, metric)
      val previousLocation = locations.getOrElse(key, Set.empty)
      locations.put(key, previousLocation + value)
      coordinates += Coordinates(db, namespace, metric)
      sender ! LocationCached(db, namespace, metric, value)
    case GetLocationsFromCache(db, namespace, metric) =>
      sender ! LocationsCached(db,
                               namespace,
                               metric,
                               locations.getOrElse(MetricLocationsCacheKey(db, namespace, metric), Set.empty).toList)
    case DeleteAll =>
      locations.clear()
      metricInfo.clear()
      coordinates.clear()
      sender() ! DeleteDone
    case PutMetricInfoInCache(info @ MetricInfo(db, namespace, metric, _, _)) =>
      val key = MetricInfoCacheKey(db, namespace, metric)
      Option(metricInfo.get(key)) match {
        case Some(v) =>
          sender ! MetricInfoAlreadyExisting(key, v)
        case None =>
          metricInfo.put(key, info)
          sender ! MetricInfoCached(db, namespace, metric, Some(info))
      }
    case EvictLocation(db, namespace, location) =>
      val key              = MetricLocationsCacheKey(db, namespace, location.metric)
      val previousLocation = locations.getOrElse(key, Set.empty)
      locations.put(key, previousLocation - location)
      sender ! Right(LocationEvicted(db, namespace, location))
    case GetMetricInfoFromCache(db, namespace, metric) =>
      val key = MetricInfoCacheKey(db, namespace, metric)
      sender ! MetricInfoCached(db, namespace, metric, Option(metricInfo.get(key)))
    case GetAllMetricInfoWithRetention =>
      sender() ! AllMetricInfoWithRetentionGot(metricInfo.values().asScala.toSet.filter(_.retention > 0))
  }
}

object LocalMetadataCache {
  case object DeleteAll
  case object DeleteDone
}
