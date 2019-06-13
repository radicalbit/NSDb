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

import akka.actor.{Actor, Props}
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.coordinator.FakeMetadataCache.{DeleteAll, DeleteDone}
import io.radicalbit.nsdb.cluster.index.MetricInfo
import io.radicalbit.nsdb.model.Location

import scala.collection.mutable

class FakeMetadataCache extends Actor {

  val locations: mutable.Map[LocationWithNodeKey, Location] = mutable.Map.empty

  val metricInfo: mutable.Map[MetricInfoCacheKey, MetricInfo] = mutable.Map.empty

  val dbs: mutable.Set[String] = mutable.Set.empty

  def receive: Receive = {
    case GetDbsFromCache =>
      sender ! DbsFromCacheGot(dbs.toSet)
    case PutLocationInCache(db, namespace, metric, from, to, value) =>
      val key = LocationWithNodeKey(db, namespace, metric, value.node, from: Long, to: Long)
      locations.put(key, value)
      dbs += db
      sender ! LocationCached(db, namespace, metric, from, to, value)
    case GetLocationsFromCache(db, namespace, metric) =>
      val key                 = MetricLocationsCacheKey(db, namespace, metric)
      val locs: Seq[Location] = locations.values.filter(_.metric == key.metric).toSeq.sortBy(_.from)
      sender ! LocationsCached(db, namespace, metric, locs)
    case DeleteAll =>
      locations.keys.foreach(k => locations.remove(k))
      metricInfo.keys.foreach(k => metricInfo.remove(k))
      sender() ! DeleteDone
    case PutMetricInfoInCache(db, namespace, metric, value) =>
      val key = MetricInfoCacheKey(db, namespace, metric)
      metricInfo.get(key) match {
        case Some(v) =>
          sender ! MetricInfoAlreadyExisting(key, v)
        case None =>
          metricInfo.put(key, value)
          sender ! MetricInfoCached(db, namespace, metric, Some(value))
      }
    case GetMetricInfoFromCache(db, namespace, metric) =>
      val key = MetricInfoCacheKey(db, namespace, metric)
      sender ! MetricInfoCached(db, namespace, metric, metricInfo.get(key))
  }
}

object FakeMetadataCache {
  def props: Props = Props(new FakeMetadataCache)

  case object DeleteAll
  case object DeleteDone
}
