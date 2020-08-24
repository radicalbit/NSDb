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

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.util.ErrorManagementUtils.partitionResponses
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.concurrent.Future

class LocalMetadataCoordinator(cache: ActorRef) extends Actor {

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.metadata-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  lazy val defaultShardingInterval: Long =
    context.system.settings.config.getDuration("nsdb.sharding.interval").toMillis

  private def getShardStartIstant(timestamp: Long, shardInterval: Long) = (timestamp / shardInterval) * shardInterval

  private def getShardEndIstant(startShard: Long, shardInterval: Long) = startShard + shardInterval

  def receive: Receive = {
    case GetDbs =>
      (cache ? GetDbsFromCache)
        .mapTo[DbsFromCacheGot]
        .map(m => DbsGot(m.dbs))
        .pipeTo(sender())
    case GetNamespaces(db) =>
      (cache ? GetNamespacesFromCache(db))
        .mapTo[NamespacesFromCacheGot]
        .map(m => NamespacesGot(db, m.namespaces))
        .pipeTo(sender())
    case GetMetrics(db, namespace) =>
      (cache ? GetMetricsFromCache(db, namespace))
        .mapTo[MetricsFromCacheGot]
        .map(m => MetricsGot(db, namespace, m.metrics))
        .pipeTo(sender())
    case DropMetric(db, namespace, metric) =>
      (cache ? DropMetricFromCache(db, namespace, metric))
        .mapTo[MetricFromCacheDropped]
        .map(_ => MetricDropped(db, namespace, metric))
        .pipeTo(sender())
    case DeleteNamespace(db, namespace) =>
      (cache ? DropNamespaceFromCache(db, namespace))
        .mapTo[NamespaceFromCacheDropped]
        .map(_ => NamespaceDeleted(db, namespace))
        .pipeTo(sender())
    case GetLocations(db, namespace, metric) =>
      (cache ? GetLocationsFromCache(db, namespace, metric))
        .mapTo[LocationsCached]
        .map(l => LocationsGot(db, namespace, metric, l.locations))
        .pipeTo(sender())
    case GetWriteLocations(db, namespace, metric, timestamp) =>
      val start = getShardStartIstant(timestamp, defaultShardingInterval)
      val end   = getShardEndIstant(start, defaultShardingInterval)

      val location = Location(metric, "localhost", start, end)

      (cache ? PutLocationInCache(db, namespace, location.metric, location))
        .map {
          case LocationCached(_, _, _, loc) => WriteLocationsGot(db, namespace, metric, Seq(loc))
          case e =>
            GetWriteLocationsFailed(db,
                                    namespace,
                                    metric,
                                    timestamp,
                                    s"unexpected response while trying to put metadata in cache $e")
        }
        .pipeTo(sender())
    case AddLocations(db, namespace, locations) =>
      Future
        .sequence(
          locations.map(location =>
            (cache ? PutLocationInCache(db, namespace, location.metric, location))
              .mapTo[AddLocationResponse]))
        .flatMap { responses =>
          val (successResponses: List[LocationCached], errorResponses: List[PutLocationInCacheFailed]) =
            partitionResponses[LocationCached, PutLocationInCacheFailed](responses)

          if (successResponses.size == responses.size) {
            Future(LocationsAdded(db, namespace, successResponses.map(_.value)))
          } else {
            Future(AddLocationsFailed(db, namespace, errorResponses.map(_.location)))
          }
        }
        .pipeTo(sender())
    case GetMetricInfo(db, namespace, metric) =>
      (cache ? GetMetricInfoFromCache(db, namespace, metric))
        .map {
          case MetricInfoCached(_, _, _, value) => MetricInfoGot(db, namespace, metric, value)
        }
        .pipeTo(sender)
    case PutMetricInfo(metricInfo) =>
      (cache ? PutMetricInfoInCache(metricInfo))
        .map {
          case MetricInfoCached(_, _, _, Some(_)) =>
            MetricInfoPut(metricInfo)
          case MetricInfoAlreadyExisting(_, _) =>
            MetricInfoFailed(metricInfo, "metric info already exist")
          case e => MetricInfoFailed(metricInfo, s"Unknown response from cache $e")
        }
        .pipeTo(sender)
  }
}

object LocalMetadataCoordinator {
  def props(cache: ActorRef): Props = Props(new LocalMetadataCoordinator(cache))
}
