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

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.client.ClusterClient.Publish
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.cluster.{Cluster, MemberStatus}
import akka.pattern._
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics.{COORDINATORS_TOPIC, NODE_GUARDIANS_TOPIC}
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.createNodeName
import io.radicalbit.nsdb.cluster.index.MetricInfoIndex
import io.radicalbit.nsdb.cluster.util.FileUtils
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.Coordinates
import io.radicalbit.nsdb.index.DirectorySupport
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.TimeRangeExtractor
import io.radicalbit.nsdb.util.ActorPathLogging
import org.apache.lucene.index.{IndexNotFoundException, IndexUpgrader}
import org.apache.lucene.store.Directory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/**
  * Actor that handles metadata (i.e. write location for metrics)
  * @param cache cluster aware metric's location cache
  */
class MetadataCoordinator(cache: ActorRef, mediator: ActorRef)
    extends ActorPathLogging
    with Stash
    with DirectorySupport {
  private val cluster = Cluster(context.system)

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.metadata-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  lazy val defaultShardingInterval: Long =
    context.system.settings.config.getDuration("nsdb.sharding.interval").toMillis

  lazy val replicationFactor: Int =
    context.system.settings.config.getInt("nsdb.cluster.replication-factor")

  lazy val retentionCheck = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.retention.check.interval").toNanos,
    TimeUnit.NANOSECONDS)

  private val metricsDataActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  context.setReceiveTimeout(retentionCheck)

  private def getShardStartIstant(timestamp: Long, shardInterval: Long) = (timestamp / shardInterval) * shardInterval

  private def getShardEndIstant(startShard: Long, shardInterval: Long) = startShard + shardInterval

  private def performAddLocationIntoCache(db: String, namespace: String, locations: Seq[Location]) =
    Future
      .sequence(
        locations.map(location =>
          (cache ? PutLocationInCache(db, namespace, location.metric, location.from, location.to, location))
            .mapTo[AddLocationResponse]))
      .flatMap { responses =>
        val (successResponses: List[LocationCached], errorResponses: List[PutLocationInCacheFailed]) =
          responses.foldRight((List.empty[LocationCached], List.empty[PutLocationInCacheFailed])) {
            case (f, (successAcc, errorAcc)) =>
              f match {
                case success: LocationCached         => (success :: successAcc, errorAcc)
                case error: PutLocationInCacheFailed => (successAcc, error :: errorAcc)
              }
          }

        if (successResponses.size == responses.size) {
          Future(LocationsAdded(db, namespace, successResponses.map(_.value)))
        } else {
          Future
            .sequence(successResponses.map(location => cache ? EvictLocation(db, namespace, location.value)))
            .flatMap { _ =>
              Future(AddLocationsFailed(db, namespace, errorResponses.map(_.location)))
            }
        }

      }

  /**
    * Retrieve the actual shard interval for a metric. If a custom interval has been configured, it will be returned,
    * otherwise the default interval (gather from the global conf file) will be used
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
          (cache ? PutMetricInfoInCache(MetricInfo(db, namespace, metric, defaultShardingInterval)))
            .map(_ => defaultShardingInterval)
      }

  override def preStart(): Unit = {
    mediator ! Subscribe(COORDINATORS_TOPIC, self)

    val interval = FiniteDuration(
      context.system.settings.config.getDuration("nsdb.publisher.scheduler.interval", TimeUnit.SECONDS),
      TimeUnit.SECONDS)

    context.system.scheduler.schedule(FiniteDuration(0, "ms"), interval) {
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetMetricsDataActors)
      log.debug("MetadataCoordinator data actor : {}", metricsDataActors.size)
    }
  }

  override def receive: Receive = {
    case ReceiveTimeout =>
      (cache ? GetAllMetricInfo)
        .mapTo[AllMetricInfoGot]
        .map {
          case AllMetricInfoGot(infos) =>
            CheckForRetention(infos.collect {
              case i if i.retention > 0 => i
            })
        }
        .pipeTo(self)
    case CheckForRetention(metricInfoes) =>
      metricInfoes.foreach {
        case MetricInfo(db, namespace, metric, _, retention) =>
          val threshold = System.currentTimeMillis() - retention

          (cache ? GetLocationsFromCache(db, namespace, metric))
            .mapTo[LocationsCached]
            .map {

              case _ @LocationsCached(_, _, _, locations) =>
                val (locationsToFullyEvict, _) =
                  TimeRangeExtractor.getLocationsToEvict(locations, threshold)

                val evictResponses = Future.sequence(locationsToFullyEvict.map { location =>
                  (cache ? EvictLocation(db, namespace, location))
                    .mapTo[Either[EvictLocationFailed, LocationEvicted]]
                    .recover { case _ => Left(EvictLocationFailed(db, namespace, location)) }
                })
                evictResponses.map { responses =>
                  val errors: ListBuffer[EvictLocationFailed] = ListBuffer.empty
                  val successes: ListBuffer[LocationEvicted]  = ListBuffer.empty

                  responses.map {
                    case Right(response) => successes += response
                    case Left(response)  => errors += response
                  }
                  (errors.toList, successes.toList)
                }
            }
      }
    case SubscribeMetricsDataActor(actor: ActorRef, nodeName) =>
      if (!metricsDataActors.get(nodeName).contains(actor)) {
        metricsDataActors += (nodeName -> actor)
        log.info(s"subscribed data actor for node $nodeName")
      }
      sender() ! MetricsDataActorSubscribed(actor, nodeName)
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
        .map { _ =>
          MetricDropped(db, namespace, metric)
        }
        .pipeTo(sender())
    case DeleteNamespace(db, namespace) =>
      (cache ? DropNamespaceFromCache(db, namespace))
        .mapTo[NamespaceFromCacheDropped]
        .map { _ =>
          NamespaceDeleted(db, namespace)
        }
        .pipeTo(sender())
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
                      case LocationsAdded(_, _, locs) => LocationsGot(db, namespace, metric, locs)
                      case _                          => GetWriteLocationsFailed(db, namespace, metric, timestamp)
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
                  case LocationsAdded(_, _, locs) => LocationsGot(db, namespace, metric, locs)
                  case _                          => GetWriteLocationsFailed(db, namespace, metric, timestamp)
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
          case MetricInfoCached(_, _, _, value) => MetricInfoGot(db, namespace, metric, value)
        }
        .pipeTo(sender)
    case PutMetricInfo(metricInfo @ MetricInfo(db, namespace, metric, _, _)) =>
      (cache ? PutMetricInfoInCache(metricInfo))
        .map {
          case MetricInfoCached(_, _, _, Some(_)) =>
            MetricInfoPut(metricInfo)
          case MetricInfoAlreadyExisting(_, _) =>
            MetricInfoFailed(metricInfo, "metric info already exist")
          case e => MetricInfoFailed(metricInfo, s"Unknown response from cache $e")
        }
        .pipeTo(sender)
    case Migrate(inputPath) =>
      val allMetadata: Seq[(Coordinates, MetricInfo)] = FileUtils.getSubDirs(inputPath).flatMap { db =>
        FileUtils.getSubDirs(db).toList.flatMap { namespace =>
          val metricInfoDirectory =
            createMmapDirectory(Paths.get(inputPath, db.getName, namespace.getName, "metadata", "info"))

          Try {
            new IndexUpgrader(metricInfoDirectory).upgrade()
          }.recover {
            case _: IndexNotFoundException => //do nothing
          }

          val metricInfoIndex = getMetricInfoIndex(metricInfoDirectory)
          val metricInfos = metricInfoIndex.all
            .map(e => Coordinates(db.getName, namespace.getName, e.metric) -> e)

          metricInfoIndex.close()

          metricInfos
        }
      }

      Future
        .sequence(allMetadata.map {
          case (Coordinates(db, namespace, _), metricInfo) =>
            (cache ? PutMetricInfoInCache(metricInfo))
              .mapTo[MetricInfoCached]
        })
        .map(seq => MetricInfosMigrated(seq.flatMap(_.value)))
        .pipeTo(sender())

  }

  private def getMetricInfoIndex(directory: Directory): MetricInfoIndex = new MetricInfoIndex(directory)
}

object MetadataCoordinator {

  object commands {

    case class GetLocations(db: String, namespace: String, metric: String)
    case class GetWriteLocations(db: String, namespace: String, metric: String, timestamp: Long)
    case class AddLocation(db: String, namespace: String, location: Location)
    case class AddLocations(db: String, namespace: String, locations: Seq[Location])
    case class DeleteLocation(db: String, namespace: String, location: Location)
    case class DeleteMetricMetadata(db: String,
                                    namespace: String,
                                    metric: String,
                                    occurredOn: Long = System.currentTimeMillis)
    case class DeleteNamespaceMetadata(db: String, namespace: String, occurredOn: Long = System.currentTimeMillis)

    case class GetMetricInfo(db: String, namespace: String, metric: String)
    case class PutMetricInfo(metricInfo: MetricInfo)

    case class CheckForRetention(metricInfo: Set[MetricInfo])
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
    case class MetricMetadataDeleted(db: String, namespace: String, metric: String, occurredOn: Long)
    case class NamespaceMetadataDeleted(db: String, namespace: String, occurredOn: Long)

    case class MetricInfoGot(db: String, namespace: String, metric: String, metricInfo: Option[MetricInfo])
    case class MetricInfoPut(metricInfo: MetricInfo)
    case class MetricInfoFailed(metricInfo: MetricInfo, message: String)

    case class MetricInfosMigrated(infos: Seq[MetricInfo])
  }

  def props(cache: ActorRef, mediator: ActorRef): Props = Props(new MetadataCoordinator(cache, mediator))
}
