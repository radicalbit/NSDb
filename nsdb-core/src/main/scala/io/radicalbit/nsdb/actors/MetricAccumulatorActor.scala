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

package io.radicalbit.nsdb.actors

import akka.actor.SupervisorStrategy.{Escalate, Resume}
import akka.actor.{ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import akka.routing.Broadcast
import akka.util.Timeout
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.MetricPerformerActor.PerformShardWrites
import io.radicalbit.nsdb.common.configuration.NSDbConfig
import io.radicalbit.nsdb.common.protocol.NSDbSerializable
import io.radicalbit.nsdb.exception.InvalidLocationsInNode
import io.radicalbit.nsdb.index.StorageStrategy
import io.radicalbit.nsdb.model.{Location, TimeContext}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.util.{ActorPathLogging, FileUtils => NSDbFileUtils}
import org.apache.commons.io.FileUtils
import org.apache.lucene.index.IndexWriter

import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Actor responsible for accumulating write and delete operations which will be performed by [[MetricPerformerActor]].
  *
  * @param basePath shards indexes path.
  * @param db shards db.
  * @param namespace shards namespace.
  */
class MetricAccumulatorActor(val basePath: String,
                             val db: String,
                             val namespace: String,
                             val readerActor: ActorRef,
                             val localCommitLogCoordinator: ActorRef)
    extends ActorPathLogging
    with MetricsActor {

  import scala.collection.mutable

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  override lazy val indexStorageStrategy: StorageStrategy =
    StorageStrategy.withValue(context.system.settings.config.getString(NSDbConfig.HighLevel.StorageStrategy))

  /**
    * Actor responsible for the actual writes into indexes.
    */
  var performerActor: ActorRef = _

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
    * Writes scheduler interval.
    */
  lazy val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  /**
    * Map containing all the accumulated operations that will be passed to the [[PerformShardWrites]].
    */
  private val opBufferMap: mutable.Map[String, ShardOperation] = mutable.Map.empty

  /**
    * operations currently being written by the [[io.radicalbit.nsdb.actors.MetricPerformerActor]].
    */
  private var performingOps: Map[String, ShardOperation] = Map.empty

  private def deleteMetricData(metric: String): Unit =
    NSDbFileUtils
      .getMetricShards(Paths.get(basePath, db, namespace, "shards"), metric)
      .foreach(
        folderName => FileUtils.deleteDirectory(Paths.get(basePath, db, namespace, "shards", folderName.getName).toFile)
      )

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: InvalidLocationsInNode =>
      log.error(e, s"Invalid locations ${e.locations} found")
      Escalate
    case t =>
      log.error(t, "generic error occurred")
      Resume
  }

  /**
    * Any existing shard is retrieved, the [[MetricPerformerActor]] is initialized and actual writes are scheduled.
    */
  override def preStart: Unit = {
    performerActor = context.actorOf(MetricPerformerActor.props(basePath, db, namespace, localCommitLogCoordinator),
                                     s"shard-performer-service-$db-$namespace")

    context.system.scheduler.schedule(0.seconds, interval) {
      if (opBufferMap.nonEmpty && performingOps.isEmpty) {
        performingOps = opBufferMap.toMap
        performerActor ! PerformShardWrites(performingOps)
      }
    }
  }

  /**
    * behaviour for ddl operations
    *
    * - [[DeleteAllMetrics]] delete all metrics.
    *
    * - [[DropMetric]] drop a given metric.
    *
    * - [[EvictShard]] delete a shard.
    *
    */
  def ddlOps: Receive = {
    case msg @ DeleteAllMetrics(_, ns) =>
      shards.foreach {
        case (_, index) =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
          index.refresh()
      }
      facetIndexShards.foreach {
        case (k, indexes) =>
          implicit val writer: IndexWriter = indexes.getIndexWriter
          indexes.deleteAll()
          writer.close()
          facetIndexShards -= k
      }

      FileUtils.deleteDirectory(Paths.get(basePath, db, namespace, "shards").toFile)

      readerActor ! Broadcast(msg)
      sender ! AllMetricsDeleted(db, ns)
    case msg @ DropMetricWithLocations(_, _, metric, locations) =>
      shardsFromLocations(locations).foreach {
        case (key, Some(index)) =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
          index.refresh()
          shards -= key
        case _ => //do nothing
      }
      facetsShardsFromLocations(locations).foreach {
        case (key, Some(indexes)) =>
          implicit val writer: IndexWriter = indexes.getIndexWriter
          indexes.deleteAll()
          writer.close()
          indexes.refresh()
          facetIndexShards -= key
        case _ => //do nothing
      }

      deleteMetricData(metric)

      readerActor ! Broadcast(msg)
      sender() ! MetricDropped(db, namespace, metric)
    case DisseminateRetention(_, _, metric, retention) =>
      metricsRetention += (metric -> retention)
    case msg @ EvictShard(_, _, location) =>
      Try {
        releaseLocation(location)
        deleteLocation(location)
      } match {
        case Success(_) =>
          readerActor ! Broadcast(msg)
          sender() ! ShardEvicted(db, namespace, location)
        case Failure(ex) =>
          sender() ! EvictedShardFailed(db, namespace, location, ex.getMessage)
      }

  }

  /**
    * behaviour for accumulate operations.
    *
    * - [[AddRecordToShard]] add a given record to a given shard.
    *
    * - [[DeleteRecordFromShard]] delete a given record from a given shard.
    *
    * - [[ExecuteDeleteStatementInShards]] execute a delete statement among the given shards.
    *
    * - [[Refresh]] refresh shard indexes after a [[PerformShardWrites]] operation executed by [[MetricPerformerActor]]
    *
    */
  def accumulate: Receive = {
    case msg @ AddRecordToShard(_, ns, location, bit) =>
      log.debug("received message {}", msg)
      opBufferMap += (UUID.randomUUID().toString -> WriteShardOperation(ns, location, bit))
      val ts = System.currentTimeMillis()
      sender ! RecordAccumulated(db, ns, location.metric, bit, location, ts)
    case DeleteRecordFromShard(_, ns, key, bit) =>
      opBufferMap += (UUID.randomUUID().toString -> DeleteShardRecordOperation(ns, key, bit))
      sender ! RecordDeleted(db, ns, key.metric, bit)
    case ExecuteDeleteStatementInShards(statement, schema, keys) =>
      implicit val timeContext: TimeContext = TimeContext()
      StatementParser.parseStatement(statement, schema) match {
        case Right(ParsedDeleteQuery(_, ns, metric, _)) =>
          keys.foreach { key =>
            opBufferMap += (UUID.randomUUID().toString -> DeleteShardQueryOperation(ns, key, statement, schema))
          }
          sender() ! DeleteStatementExecuted(db, namespace, metric)
        case Left(errorMessage) =>
          sender() ! DeleteStatementFailed(db = db, namespace = namespace, metric = statement.metric, errorMessage)
      }
    case msg @ Refresh(writeIds, keys) =>
      garbageCollectIndexes()
      opBufferMap --= writeIds
      performingOps = Map.empty
      keys.foreach { key =>
        getOrCreateIndex(key).refresh()
        getOrCreatefacetIndexesFor(key).refresh()
      }
      garbageCollectIndexes()
      readerActor ! Broadcast(msg)
  }

  override def receive: Receive = {
    ddlOps orElse accumulate
  }
}

object MetricAccumulatorActor {

  case class Refresh(writeIds: Seq[String], locations: Seq[Location]) extends NSDbSerializable

  def props(basePath: String,
            db: String,
            namespace: String,
            readerActor: ActorRef,
            localCommitLogCoordinator: ActorRef): Props =
    Props(new MetricAccumulatorActor(basePath, db, namespace, readerActor, localCommitLogCoordinator))
}
