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

package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.ddata.DurableStore.{LoadAll, LoadData}
import akka.cluster.ddata._
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.cluster.{Cluster, MemberStatus}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.PubSubTopics.{COORDINATORS_TOPIC, NODE_GUARDIANS_TOPIC}
import io.radicalbit.nsdb.cluster.actor.ClusterListener.{GetNodeMetrics, NodeMetricsGot}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.ExecuteDeleteStatementInternalInLocations
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.actor.ReplicatedSchemaCache.SchemaKey
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.deleteStatementFromThreshold
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.createNodeName
import io.radicalbit.nsdb.cluster.logic.CapacityWriteNodesSelectionLogic
import io.radicalbit.nsdb.cluster.util.ErrorManagementUtils._
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.configuration.NSDbConfig
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.{Coordinates, NSDbSerializable}
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{DirectorySupport, StorageStrategy}
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.TimeRangeManager
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

/**
  * Actor that handles locations (shards) for metrics.
  * @param clusterListener actor that collects cluster metrics.
  * @param metadataCache cluster aware metrics location cache.
  */
class MetadataCoordinator(clusterListener: ActorRef, metadataCache: ActorRef, schemaCache: ActorRef, mediator: ActorRef)
    extends ActorPathLogging
    with DirectorySupport {
  private val cluster = Cluster(context.system)

  private val config = context.system.settings.config

  implicit val timeout: Timeout =
    Timeout(config.getDuration("nsdb.metadata-coordinator.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  import context.dispatcher

  override lazy val indexStorageStrategy: StorageStrategy =
    StorageStrategy.withValue(context.system.settings.config.getString(NSDbConfig.HighLevel.StorageStrategy))

  lazy val defaultShardingInterval: Long =
    config.getDuration("nsdb.sharding.interval").toMillis

  lazy val replicationFactor: Int =
    config.getInt("nsdb.cluster.replication-factor")

  lazy val retentionCheckInterval: FiniteDuration =
    FiniteDuration(config.getDuration("nsdb.retention.check.interval").toNanos, TimeUnit.NANOSECONDS)

  private val metricsDataActors: mutable.Map[String, ActorRef]     = mutable.Map.empty
  private val commitLogCoordinators: mutable.Map[String, ActorRef] = mutable.Map.empty

  private def getShardStartInstant(timestamp: Long, shardInterval: Long) = (timestamp / shardInterval) * shardInterval

  private def getShardEndInstant(startShard: Long, shardInterval: Long) = startShard + shardInterval

  private def performAddLocationIntoCache(db: String, namespace: String, locations: Seq[Location]) =
    Future
      .sequence(
        locations.map(location =>
          (metadataCache ? PutLocationInCache(db, namespace, location.metric, location))
            .mapTo[AddLocationResponse]))
      .flatMap { responses =>
        val (successResponses: List[LocationCached], errorResponses: List[PutLocationInCacheFailed]) =
          partitionResponses[LocationCached, PutLocationInCacheFailed](responses)

        if (successResponses.size == responses.size) {
          Future(LocationsAdded(db, namespace, successResponses.map(_.value)))
        } else {
          log.error(s"errors in adding locations in cache $errorResponses")
          Future
            .sequence(successResponses.map(location => metadataCache ? EvictLocation(db, namespace, location.value)))
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
    (metadataCache ? GetMetricInfoFromCache(db, namespace, metric))
      .flatMap {
        case MetricInfoCached(_, _, _, Some(MetricInfo(_, _, _, shardInterval, _))) if shardInterval > 0 =>
          Future(shardInterval)
        case _ => Future(defaultShardingInterval)
      }

  override def preStart(): Unit = {
    mediator ! Subscribe(COORDINATORS_TOPIC, self)

    val interval =
      FiniteDuration(config.getDuration("nsdb.publisher.scheduler.interval", TimeUnit.SECONDS), TimeUnit.SECONDS)

    context.system.scheduler.schedule(FiniteDuration(0, "ms"), interval) {
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetMetricsDataActors)
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetCommitLogCoordinators)
      log.debug("WriteCoordinator data actor : {}", metricsDataActors.size)
      log.debug("WriteCoordinator commit log  actor : {}", commitLogCoordinators.size)
    }

    context.system.scheduler.schedule(FiniteDuration(0, "ms"), retentionCheckInterval) {
      metadataCache ! GetAllMetricInfoWithRetention
    }

    context.system.scheduler.schedule(FiniteDuration(0, "ms"), retentionCheckInterval * 10) {
      self ! CheckOutdatedLocations
    }
  }

  /**
    * Writes the evict (delete) operation into commit log.
    * @param db the db to be written.
    * @param namespace the namespace to be written.
    *                  @param ts the time instant of the commit log operation
    * @param location the location to be evicted.
    * @return the result of the operation.
    */
  private def partiallyEvictFromLocationCommitLogAck(db: String,
                                                     namespace: String,
                                                     ts: Long,
                                                     location: Location,
                                                     statement: DeleteSQLStatement): Future[CommitLogResponse] = {
    commitLogCoordinators.get(location.node) match {
      case Some(commitLogCoordinator) =>
        (commitLogCoordinator ? WriteToCommitLog(
          db = db,
          namespace = namespace,
          metric = location.metric,
          ts = ts,
          action = DeleteAction(statement),
          location
        )).mapTo[CommitLogResponse]
      case None =>
        //in case there is not a commit log for that node, we give back a success.
        //the location will be processed afterwards until a commit log for the node will be available again
        Future(WriteToCommitLogSucceeded(db, namespace, ts, location.metric, location))
    }
  }

  /**
    * Evict a shard physically from FS.
    * @param db the db to be written.
    * @param namespace the namespace to be written.
    *                  @param ts the time instant of the commit log operation
    * @param location the location to be evicted.
    * @return the result of the operation.
    */
  private def shardEvictCommitLogAck(db: String,
                                     namespace: String,
                                     ts: Long,
                                     location: Location): Future[CommitLogResponse] = {
    commitLogCoordinators.get(location.node) match {
      case Some(commitLogCoordinator) =>
        (commitLogCoordinator ? WriteToCommitLog(
          db = db,
          namespace = namespace,
          metric = location.metric,
          ts = ts,
          action = EvictShardAction(location),
          location
        )).mapTo[CommitLogResponse]
      case None =>
        //in case there is not a commit log for that node, we give back a success.
        //the location will be processed afterwards until a commit log for the node will be available again
        Future(WriteToCommitLogSucceeded(db, namespace, ts, location.metric, location))
    }
  }

  /**
    * Performs a partial eviction in a location by executing a [[DeleteSQLStatement]].
    * @param statement the delete statement.
    * @param location the location to be partially evicted.
    * @return the result of the delete operation.
    */
  private def partiallyEvictPerform(
      statement: DeleteSQLStatement,
      location: Location): Future[Either[DeleteStatementFailed, DeleteStatementExecuted]] = {

    (schemaCache ? GetSchemaFromCache(statement.db, statement.namespace, statement.metric))
      .flatMap {
        case SchemaCached(_, _, _, Some(schema)) =>
          metricsDataActors.get(location.node) match {
            case Some(dataActor) =>
              (dataActor ? ExecuteDeleteStatementInternalInLocations(statement, schema, Seq(location))).map {
                case msg: DeleteStatementExecuted => Right(msg)
                case msg: DeleteStatementFailed   => Left(msg)
                case msg =>
                  Left(
                    DeleteStatementFailed(statement.db,
                                          statement.namespace,
                                          statement.metric,
                                          s"unknown response from server $msg"))
              }
            case None =>
              //in case there is not a data actor for that node, we give back a success.
              //the location will be processed afterwards until a commit log for the node will be available again
              Future(Right(DeleteStatementExecuted(statement.db, statement.namespace, statement.metric)))
          }
        case _ =>
          log.warning("trying to evict a metric with locations and without schema")
          Future(
            Left(
              DeleteStatementFailed(statement.db,
                                    statement.namespace,
                                    statement.metric,
                                    s"No schema found for metric ${statement.metric}")))
      }

  }

  /**
    * Performs a partial eviction in a location by executing a [[DeleteSQLStatement]].
    * @param db the location db.
    * @param namespace the location namespace.
    * @param location the location to be partially evicted.
    * @return the result of the delete operation.
    */
  private def fullyEvictPerform(db: String,
                                namespace: String,
                                location: Location): Future[Either[EvictedShardFailed, ShardEvicted]] = {

    metricsDataActors.get(location.node) match {
      case Some(dataActor) =>
        (dataActor ? EvictShard(db, namespace, location)).map {
          case msg: ShardEvicted       => Right(msg)
          case msg: EvictedShardFailed => Left(msg)
          case msg =>
            Left(EvictedShardFailed(db, namespace, location, s"unknown response from server $msg"))
        }
      case None =>
        //in case there is not a data actor for that node, we give back a success.
        //the location will be processed afterwards until a commit log for the node will be available again
        Future(Right(ShardEvicted(db, namespace, location)))
    }
  }

  override def receive: Receive = {
    case CheckOutdatedLocations =>
      (metadataCache ? GetAllMetricInfoWithRetention).mapTo[AllMetricInfoWithRetentionGot].foreach {
        case AllMetricInfoWithRetentionGot(metricInfos) =>
          metricInfos.foreach {
            case MetricInfo(db, namespace, metric, _, _) =>
              (metadataCache ? GetLocationsFromCache(db, namespace, metric))
                .mapTo[LocationsCached]
                .foreach {
                  case LocationsCached(db, namespace, _, locations) =>
                    //check for outdated locations still written on disk
                    locations.groupBy(_.node).foreach {
                      case (node, locations) =>
                        metricsDataActors.get(node) match {
                          case Some(metricDataActor) =>
                            metricDataActor ! CheckForOutDatedShards(db, namespace, locations)
                          case None => log.debug("no metrics data actor found for node {}", node)
                        }
                    }
                }
          }
      }
    case AllMetricInfoWithRetentionGot(metricInfoes) =>
      log.debug(s"check for retention for {}", metricInfoes)
      metricInfoes.foreach {
        case MetricInfo(db, namespace, metric, _, retention) =>
          val threshold = System.currentTimeMillis() - retention

          (metadataCache ? GetLocationsFromCache(db, namespace, metric))
            .mapTo[LocationsCached]
            .map {
              case LocationsCached(_, _, _, locations) =>
                val (locationsToFullyEvict, locationsToPartiallyEvict) =
                  TimeRangeManager.getLocationsToEvict(locations, threshold)

                val cacheResponses = Future
                  .sequence(locationsToFullyEvict.map { location =>
                    (metadataCache ? EvictLocation(db, namespace, location))
                      .mapTo[Either[EvictLocationFailed, LocationEvicted]]
                      .recover { case _ => Left(EvictLocationFailed(db, namespace, location)) }
                  })
                  .map { responses =>
                    manageErrors(responses) { errors =>
                      log.error("errors during delete locations from cache {}", errors)
                      context.system.terminate()
                    }
                  }

                val cacheResponsesAckedInCommitLog = cacheResponses.flatMap { locationEvictedFromCache =>
                  Future
                    .sequence(locationEvictedFromCache.map(locationEvicted =>
                      shardEvictCommitLogAck(db, namespace, System.currentTimeMillis(), locationEvicted.location)))
                    .map { responses =>
                      manageErrors[WriteToCommitLogSucceeded, WriteToCommitLogFailed](responses) { errors =>
                        log.error("errors during delete locations from cache {}", errors)
                        context.system.terminate()
                      }
                    }
                  Future(locationEvictedFromCache)
                }

                val commitLogResponses = cacheResponsesAckedInCommitLog.flatMap { res =>
                  log.debug("evicted locations {}", res.map(_.location))
                  Future
                    .sequence(
                      locationsToPartiallyEvict.map(
                        location =>
                          partiallyEvictFromLocationCommitLogAck(
                            db,
                            namespace,
                            System.currentTimeMillis(),
                            location,
                            deleteStatementFromThreshold(db, namespace, location.metric, threshold))))
                    .map { responses =>
                      manageErrors[WriteToCommitLogSucceeded, WriteToCommitLogFailed](responses) { errors =>
                        log.error("errors during delete locations from cache {}", errors)
                        context.system.terminate()
                      }
                    }
                }

                commitLogResponses.flatMap { _ =>
                  Future.sequence(
                    locationsToPartiallyEvict.map(location =>
                      partiallyEvictPerform(deleteStatementFromThreshold(db, namespace, location.metric, threshold),
                                            location))
                  )

                  Future.sequence(
                    locationsToFullyEvict.map(location => fullyEvictPerform(db, namespace, location))
                  )
                }
            }
      }
    case SubscribeMetricsDataActor(actor: ActorRef, nodeName) =>
      if (!metricsDataActors.get(nodeName).contains(actor)) {
        metricsDataActors += (nodeName -> actor)
        log.info(s"subscribed data actor for node $nodeName")
      }
      sender() ! MetricsDataActorSubscribed(actor, nodeName)
    case SubscribeCommitLogCoordinator(actor: ActorRef, nodeName) =>
      if (!commitLogCoordinators.get(nodeName).contains(actor)) {
        commitLogCoordinators += (nodeName -> actor)
        log.info(s"subscribed commit log actor for node $nodeName")
      }
      sender() ! CommitLogCoordinatorSubscribed(actor, nodeName)

    case UnsubscribeMetricsDataActor(nodeName) =>
      metricsDataActors -= nodeName
      log.info(s"metric data actor removed for node $nodeName")
      sender() ! MetricsDataActorUnSubscribed(nodeName)
    case UnSubscribeCommitLogCoordinator(nodeName) =>
      commitLogCoordinators -= nodeName
      log.info(s"unsubscribed commit log actor for node $nodeName")
      sender() ! CommitLogCoordinatorUnSubscribed(nodeName)
    case GetDbs =>
      (metadataCache ? GetDbsFromCache)
        .mapTo[DbsFromCacheGot]
        .map(m => DbsGot(m.dbs))
        .pipeTo(sender())
    case GetNamespaces(db) =>
      (metadataCache ? GetNamespacesFromCache(db))
        .mapTo[NamespacesFromCacheGot]
        .map(m => NamespacesGot(db, m.namespaces))
        .pipeTo(sender())
    case GetMetrics(db, namespace) =>
      (metadataCache ? GetMetricsFromCache(db, namespace))
        .mapTo[MetricsFromCacheGot]
        .map(m => MetricsGot(db, namespace, m.metrics))
        .pipeTo(sender())
    case DropMetric(db, namespace, metric) =>
      (metadataCache ? DropMetricFromCache(db, namespace, metric))
        .mapTo[MetricFromCacheDropped]
        .map { _ =>
          MetricDropped(db, namespace, metric)
        }
        .pipeTo(sender())
    case DeleteNamespace(db, namespace) =>
      (metadataCache ? DropNamespaceFromCache(db, namespace))
        .mapTo[NamespaceFromCacheDropped]
        .map { _ =>
          NamespaceDeleted(db, namespace)
        }
        .pipeTo(sender())
    case GetLocations(db, namespace, metric) =>
      (metadataCache ? GetLocationsFromCache(db, namespace, metric))
        .mapTo[LocationsCached]
        .map(l => LocationsGot(db, namespace, metric, l.value))
        .pipeTo(sender())
    case GetWriteLocations(db, namespace, metric, timestamp) =>
      (metadataCache ? GetLocationsFromCache(db, namespace, metric))
        .flatMap {
          case LocationsCached(_, _, _, values) =>
            values.filter(v => v.from <= timestamp && v.to >= timestamp) match {
              case Nil =>
                for {
                  shardInterval <- getShardInterval(db, namespace, metric)
                  nodeMetrics   <- (clusterListener ? GetNodeMetrics).mapTo[NodeMetricsGot]
                  addLocationResult <- {
                    val start = getShardStartInstant(timestamp, shardInterval)
                    val end   = getShardEndInstant(start, shardInterval)

                    val nodes =
                      if (nodeMetrics.nodeMetrics.nonEmpty)
                        new CapacityWriteNodesSelectionLogic(
                          CapacityWriteNodesSelectionLogic.fromConfigValue(
                            config.getString("nsdb.cluster.metrics-selector")))
                          .selectWriteNodes(nodeMetrics.nodeMetrics, replicationFactor)
                      else {
                        Random
                          .shuffle(cluster.state.members.filter(_.status == MemberStatus.Up))
                          .take(replicationFactor)
                          .map(createNodeName)
                      }

                    val locations = nodes.map(Location(metric, _, start, end)).toSeq
                    performAddLocationIntoCache(db, namespace, locations)
                  }
                } yield
                  addLocationResult match {
                    case LocationsAdded(_, _, locs) => LocationsGot(db, namespace, metric, locs)
                    case e =>
                      log.error(s"unexpected result while trying to add locations in cache $e")
                      GetWriteLocationsFailed(db, namespace, metric, timestamp)
                  }
              case s => Future(LocationsGot(db, namespace, metric, s))
            }
          case _ =>
            Future(LocationsGot(db, namespace, metric, Seq.empty))
        } pipeTo sender()
    case AddLocation(db, namespace, location) =>
      performAddLocationIntoCache(db, namespace, Seq(location)).pipeTo(sender)
    case AddLocations(db, namespace, locations) =>
      performAddLocationIntoCache(db, namespace, locations).pipeTo(sender)
    case GetMetricInfo(db, namespace, metric) =>
      (metadataCache ? GetMetricInfoFromCache(db, namespace, metric))
        .map {
          case MetricInfoCached(_, _, _, value) => MetricInfoGot(db, namespace, metric, value)
        }
        .pipeTo(sender)
    case PutMetricInfo(metricInfo: MetricInfo) =>
      (metadataCache ? PutMetricInfoInCache(metricInfo))
        .map {
          case MetricInfoCached(_, _, _, Some(_)) =>
            MetricInfoPut(metricInfo)
          case MetricInfoAlreadyExisting(_, _) =>
            MetricInfoFailed(metricInfo, "metric info already exist")
          case e => MetricInfoFailed(metricInfo, s"Unknown response from cache $e")
        }
        .pipeTo(sender)
    case RemoveNodeMetadata(nodeName) =>
      log.info(s"remove locations for node $nodeName")
      (metadataCache ? EvictLocationsInNode(nodeName))
        .map {
          case Left(EvictLocationsInNodeFailed(_)) => RemoveNodeMetadataFailed(nodeName)
          case Right(LocationsInNodeEvicted(_))    => NodeMetadataRemoved(nodeName)
        }
        .pipeTo(sender())
    case ExecuteRestoreMetadata(path: String) =>
      val temporaryDurableStoreActor = context.actorOf(
        LmdbDurableStore.props(ConfigFactory.parseString(s"""
           |lmdb {
           |  write-behind-interval = off
           |  map-size = 100 MiB
           |  dir = $path
           |}
           |""".stripMargin)),
        name = s"metadata_restorer_${path.replaceAll("/", "_")}"
      )

      (temporaryDurableStoreActor ? LoadAll)
        .map {
          case LoadData(data) =>
            log.debug(s"restoring $data")
            log.info(s"restoring ${data.size} metadata entries")

            data.foreach {
              case ("coordinates-cache", envelope: DurableStore.DurableDataEnvelope) =>
                envelope.data.asInstanceOf[ORSet[Coordinates]].elements.foreach {
                  case Coordinates(db, namespace, metric) =>
                    metadataCache ! PutCoordinateInCache(db, namespace, metric)
                }
              case ("all-metric-info-cache", envelope: DurableStore.DurableDataEnvelope) =>
                envelope.data.asInstanceOf[ORSet[MetricInfo]].elements.foreach { info: MetricInfo =>
                  metadataCache ! PutMetricInfoInCache(info)
                }
              case (key, envelope) if key.startsWith("schema-cache") =>
                envelope.data.asInstanceOf[LWWMap[SchemaKey, Schema]].entries.foreach {
                  case (SchemaKey(db, namespace, metric), schema) =>
                    schemaCache ! PutSchemaInCache(db, namespace, metric, schema)
                }
              case (key, _) => log.debug(s"cache key $key not required for restoring")
            }

            MetadataRestored(path)
          case e =>
            val reason = s"unexpected response while trying to restore metadata $e"
            log.error(reason)
            temporaryDurableStoreActor ! PoisonPill
            RestoreMetadataFailed(path, reason)
        }
        .pipeTo(sender())
  }

}

object MetadataCoordinator {

  /**
    * Generates a delete statement given a threshold. The delete statement involves records older than the threshold.
    * e.g. delete from "metric" where timestamp < threshold
    */
  def deleteStatementFromThreshold(db: String, namespace: String, metric: String, threshold: Long): DeleteSQLStatement =
    DeleteSQLStatement(
      db = db,
      namespace = namespace,
      metric = metric,
      condition = Condition(
        ComparisonExpression(dimension = "timestamp",
                             comparison = LessThanOperator,
                             value = AbsoluteComparisonValue(threshold)))
    )

  object commands {

    case class GetLocations(db: String, namespace: String, metric: String) extends NSDbSerializable
    case class GetWriteLocations(db: String, namespace: String, metric: String, timestamp: Long)
        extends NSDbSerializable
    case class AddLocation(db: String, namespace: String, location: Location)        extends NSDbSerializable
    case class AddLocations(db: String, namespace: String, locations: Seq[Location]) extends NSDbSerializable
    case class DeleteMetricMetadata(db: String,
                                    namespace: String,
                                    metric: String,
                                    occurredOn: Long = System.currentTimeMillis)
        extends NSDbSerializable
    case class PutMetricInfo(metricInfo: MetricInfo) extends NSDbSerializable

    case object CheckOutdatedLocations extends NSDbSerializable

    case class RemoveNodeMetadata(nodeName: String) extends NSDbSerializable

    case class ExecuteRestoreMetadata(path: String) extends NSDbSerializable
  }

  object events {

    case class LocationsGot(db: String, namespace: String, metric: String, locations: Seq[Location])
        extends NSDbSerializable
    case class GetWriteLocationsFailed(db: String, namespace: String, metric: String, timestamp: Long)
        extends NSDbSerializable
    case class UpdateLocationFailed(db: String, namespace: String, oldLocation: Location, newOccupation: Long)
        extends NSDbSerializable
    case class LocationAdded(db: String, namespace: String, location: Location)            extends NSDbSerializable
    case class AddLocationFailed(db: String, namespace: String, location: Location)        extends NSDbSerializable
    case class LocationsAdded(db: String, namespace: String, locations: Seq[Location])     extends NSDbSerializable
    case class AddLocationsFailed(db: String, namespace: String, locations: Seq[Location]) extends NSDbSerializable
    case class MetricMetadataDeleted(db: String, namespace: String, metric: String, occurredOn: Long)
        extends NSDbSerializable

    case class MetricInfoPut(metricInfo: MetricInfo)                     extends NSDbSerializable
    case class MetricInfoFailed(metricInfo: MetricInfo, message: String) extends NSDbSerializable

    case class MetricInfosMigrated(infos: Seq[MetricInfo]) extends NSDbSerializable

    trait RemoveNodeMetadataResponse                      extends NSDbSerializable
    case class NodeMetadataRemoved(nodeName: String)      extends RemoveNodeMetadataResponse
    case class RemoveNodeMetadataFailed(nodeName: String) extends RemoveNodeMetadataResponse

    trait RestoreMetadataResponse                                  extends NSDbSerializable
    case class MetadataRestored(path: String)                      extends RestoreMetadataResponse
    case class RestoreMetadataFailed(path: String, reason: String) extends RestoreMetadataResponse
  }

  def props(clusterListener: ActorRef, metadataCache: ActorRef, schemaCache: ActorRef, mediator: ActorRef): Props =
    Props(new MetadataCoordinator(clusterListener, metadataCache, schemaCache, mediator))
}
