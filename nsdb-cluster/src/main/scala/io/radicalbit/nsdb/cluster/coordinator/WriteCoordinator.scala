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

import java.io.File
import java.nio.file.Paths
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props, Stash}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.dispatch.ControlMessage
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics.{COORDINATORS_TOPIC, NODE_GUARDIANS_TOPIC}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.{
  AddRecordToLocation,
  ExecuteDeleteStatementInternalInLocations
}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{
  AddLocation,
  GetLocations,
  GetWriteLocations
}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsGot
import io.radicalbit.nsdb.cluster.coordinator.WriteCoordinator._
import io.radicalbit.nsdb.cluster.util.FileUtils
import io.radicalbit.nsdb.cluster.{NsdbPerfLogger, createNodeName}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.index.{SchemaIndex, TimeSeriesIndex}
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.rpc.dump.DumpTarget
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._
import org.apache.commons.io.{FileUtils => ApacheFileUtils}
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{MatchAllDocsQuery, Sort, SortField}
import org.apache.lucene.store.MMapDirectory
import org.zeroturnaround.zip.ZipUtil

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object WriteCoordinator {

  def props(metadataCoordinator: ActorRef,
            schemaCoordinator: ActorRef,
            publisherActor: ActorRef,
            mediator: ActorRef): Props =
    Props(new WriteCoordinator(metadataCoordinator, schemaCoordinator, publisherActor, mediator))

  case class Restore(path: String)  extends ControlMessage
  case class Restored(path: String) extends ControlMessage

  case class CreateDump(inputPath: String, targets: Seq[DumpTarget]) extends ControlMessage
  case class DumpCreated(inputPath: String)                          extends ControlMessage

}

/**
  * Actor that receives every write (or delete) request and coordinates them among internal data storage.
  * @param metadataCoordinator  [[MetadataCoordinator]] the metadata coordinator.
  * @param schemaCoordinator   [[SchemaCoordinator]] the namespace schema actor.
  * @param publisherActor       [[io.radicalbit.nsdb.actors.PublisherActor]] the publisher actor.
  */
class WriteCoordinator(metadataCoordinator: ActorRef,
                       schemaCoordinator: ActorRef,
                       publisherActor: ActorRef,
                       mediator: ActorRef)
    extends ActorPathLogging
    with NsdbPerfLogger
    with Stash {

  import akka.pattern.ask

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  val commitLogEnabled: Boolean = context.system.settings.config.getBoolean("nsdb.commit-log.enabled")
  log.info("WriteCoordinator is ready.")
  if (!commitLogEnabled)
    log.info("Commit Log is disabled")

  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  lazy val consistencyLevel: Int =
    context.system.settings.config.getInt("nsdb.cluster.consistency-level")

  private val metricsDataActors: mutable.Map[String, ActorRef]     = mutable.Map.empty
  private val commitLogCoordinators: mutable.Map[String, ActorRef] = mutable.Map.empty

  /**
    * Performs an ask to every namespace actor subscribed.
    * @param msg the message to be sent.
    * @return the result of the broadcast.
    */
  private def broadcastMessage(msg: Any) =
    Future
      .sequence(metricsDataActors.values.toSeq.map(actor => actor ? msg))
      .map(_.head)

  /**
    * Writes the bit into commit log.
    * @param db the db to be written.
    * @param namespace the namespace to be written.
    * @param metric the metric to be written.
    * @param action the action to be written.
    * @return the result of the operation.
    */
  private def writeCommitLog(db: String,
                             namespace: String,
                             ts: Long,
                             metric: String,
                             nodeName: String,
                             action: CommitLoggerAction): Future[CommitLogResponse] = {
    if (commitLogEnabled && commitLogCoordinators.get(nodeName).isDefined)
      (commitLogCoordinators(nodeName) ? WriteToCommitLog(db = db,
                                                          namespace = namespace,
                                                          metric = metric,
                                                          ts = ts,
                                                          action = action))
        .mapTo[CommitLogResponse]
    else if (commitLogEnabled) {
      Future.successful(
        WriteToCommitLogFailed(db, namespace, ts, metric, "CommitLog enabled but not defined, shutting down"))
    } else Future.successful(WriteToCommitLogSucceeded(db = db, namespace = namespace, ts, metric))
  }

  /**
    * Check if a bit has got a valid schema and, in case of success, updates the metric's schema.
    * @param db the db.
    * @param namespace the namespace.
    * @param metric the namespace.
    * @param bit the bit containing the new schema.
    * @param op code executed in case of success.
    */
  def updateSchema(db: String, namespace: String, metric: String, bit: Bit)(op: Schema => Future[Any]): Future[Any] =
    (schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, metric, bit))
      .flatMap {
        case SchemaUpdated(_, _, _, schema) =>
          log.debug("Valid schema for the metric {} and the bit {}", metric, bit)
          op(schema)
        case UpdateSchemaFailed(_, _, _, errs) =>
          log.error("Invalid schema for the metric {} and the bit {}. Error are {}.", metric, bit, errs.mkString(","))
          Future(RecordRejected(db, namespace, metric, bit, errs))
      }

  /**
    * Gets the metadata locations to write the bit in. The number of location returned depends on the configured replication factor
    * @param db the db.
    * @param namespace the namespace.
    * @param metric the metric.
    * @param bit the bit.
    * @param ts the timestamp of the write operation.
    * @param op code executed in case of success.
    * @return [[LocationsGot]] if communication with metadata coordinator succeeds, RecordRejected otherwise
    */
  def getMetadataLocations(db: String, namespace: String, metric: String, bit: Bit, ts: Long)(
      op: Seq[Location] => Future[Any]): Future[Any] =
    (metadataCoordinator ? GetWriteLocations(db, namespace, metric, ts)).flatMap {
      case LocationsGot(_, _, _, locations) =>
        log.debug(s"received locations for metric $metric, $locations")
        op(locations)
      case _ =>
        log.error(s"no location found for bit $bit")
        Future(RecordRejected(db, namespace, metric, bit, List(s"no location found for bit $bit")))
    }

  /**
    * Enqueues the bit into an internal structure. The real write is performed afterwards.
    * @param db the db.
    * @param namespace the namespace.
    * @param metric the metric.
    * @param bit the bit.
    * @param location the location to write the bit in.
    */
  def accumulateRecord(db: String, namespace: String, metric: String, bit: Bit, location: Location): Future[Any] =
    metricsDataActors.get(location.node) match {
      case Some(actor) =>
        (actor ? AddRecordToLocation(db, namespace, bit, location)).map {
          case msg: RecordAdded    => msg
          case msg: RecordRejected => msg
          case _ =>
            RecordRejected(db, namespace, metric, bit, List("unknown response from metrics Actor"))
        }
      case None =>
        log.error(s"no data actor for node ${location.node}")
        Future(RecordRejected(db, namespace, metric, bit, List(s"no data actor for node ${location.node}")))
    }

  override def preStart(): Unit = {
    mediator ! Subscribe(COORDINATORS_TOPIC, self)

    val interval = FiniteDuration(
      context.system.settings.config.getDuration("nsdb.publisher.scheduler.interval", TimeUnit.SECONDS),
      TimeUnit.SECONDS)

    context.system.scheduler.schedule(FiniteDuration(0, "ms"), interval) {
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetMetricsDataActors)
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetCommitLogCoordinators)
      log.debug("writecoordinator data actor : {}", metricsDataActors.size)
      log.debug("writecoordinator commit log  actor : {}", commitLogCoordinators.size)
    }
  }

  override def receive: Receive = warmUp

  /**
    * Initial state in which actor waits metadata warm-up completion.
    */
  def warmUp: Receive = {
    case WarmUpCompleted =>
      unstashAll()
      context.become(operative)
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
    case GetConnectedDataNodes =>
      sender ! ConnectedDataNodesGot(metricsDataActors.keys.toSeq)
    case msg =>
      stash()
      log.error(s"Received and stashed message $msg during warmUp")
  }

  def operative: Receive = {
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
    case GetConnectedDataNodes =>
      sender ! ConnectedDataNodesGot(metricsDataActors.keys.toSeq)
    case MapInput(ts, db, namespace, metric, bit) =>
      val startTime = System.currentTimeMillis()
      log.debug("Received a write request for (ts: {}, metric: {}, bit : {})", ts, metric, bit)
      getMetadataLocations(db, namespace, metric, bit, bit.timestamp) { locations =>
        updateSchema(db, namespace, metric, bit) { schema =>
          val consistentLocations = locations.take(consistencyLevel)
          val eventualLocations   = locations.diff(consistentLocations)

          val consistentResult = Future
            .sequence(consistentLocations.map { loc =>
              writeCommitLog(db, namespace, bit.timestamp, metric, loc.node, InsertAction(bit))
                .flatMap {
                  case WriteToCommitLogSucceeded(_, _, _, _) =>
                    accumulateRecord(db, namespace, metric, bit, loc)
                  case err: WriteToCommitLogFailed => Future(err)
                }
            })
            .map {
              case _: Seq[RecordAdded] =>
                publisherActor ! PublishRecord(db, namespace, metric, bit, schema)
                InputMapped(db, namespace, metric, bit)
              case r: Seq[Any] =>
                val toCompensateStage = r.filter(_.isInstanceOf[RecordAdded])
                //FIXME add compensation
                val toCompensateCL = r.filter(!_.isInstanceOf[WriteToCommitLogFailed])
                //FIXME add compensation
                RecordRejected(db, namespace, metric, bit, List(""))
            }

          Future
            .sequence(eventualLocations.map { loc =>
              writeCommitLog(db, namespace, bit.timestamp, metric, loc.node, InsertAction(bit))
                .flatMap {
                  case WriteToCommitLogSucceeded(_, _, _, _) =>
                    accumulateRecord(db, namespace, metric, bit, loc)
                  case err: WriteToCommitLogFailed => Future(err)
                }
            })

          consistentResult
        }
      }.pipeToWithEffect(sender()) { _ =>
        if (perfLogger.isDebugEnabled)
          perfLogger.debug("End write request in {} millis", System.currentTimeMillis() - startTime)
      }
    case msg @ DeleteNamespace(db, namespace) =>
      writeCommitLog(db, namespace, System.currentTimeMillis(), "", "", DeleteNamespaceAction)
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (metricsDataActors.isEmpty) {
              (schemaCoordinator ? msg).map(_ => NamespaceDeleted(db, namespace))
            } else
              (schemaCoordinator ? msg).flatMap(_ => broadcastMessage(msg))
          case WriteToCommitLogFailed(_, _, _, _, reason) =>
            log.error(s"Failed to write to commit-log for: $msg with reason: $reason")
            context.system.terminate()
        }
        .pipeTo(sender())
    case msg @ ExecuteDeleteStatement(statement @ DeleteSQLStatement(db, namespace, metric, _)) =>
      writeCommitLog(db, namespace, System.currentTimeMillis(), metric, "", DeleteAction(statement))
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (metricsDataActors.isEmpty)
              Future(DeleteStatementExecuted(statement.db, statement.metric, statement.metric))
            else
              (schemaCoordinator ? GetSchema(statement.db, statement.namespace, statement.metric))
                .flatMap {
                  case SchemaGot(_, _, _, Some(schema)) =>
                    (metadataCoordinator ? GetLocations(db, namespace, metric)).flatMap {
                      case LocationsGot(_, _, _, locations) if locations.isEmpty =>
                        Future(DeleteStatementExecuted(statement.db, statement.metric, statement.metric))
                      case LocationsGot(_, _, _, locations) =>
                        broadcastMessage(ExecuteDeleteStatementInternalInLocations(statement, schema, locations))
                      case _ =>
                        Future(
                          DeleteStatementFailed(db,
                                                namespace,
                                                metric,
                                                s"Unable to fetch locations for metric ${statement.metric}"))
                    }
                  case _ =>
                    Future(
                      DeleteStatementFailed(db, namespace, metric, s"No schema found for metric ${statement.metric}"))
                }
          case WriteToCommitLogFailed(_, _, _, _, reason) =>
            log.error(s"Failed to write to commit-log for: $msg with reason: $reason")
            context.system.terminate()
        }
        .pipeTo(sender())
    case msg @ DropMetric(db, namespace, metric) =>
      writeCommitLog(db, namespace, System.currentTimeMillis(), metric, "", DeleteMetricAction)
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (metricsDataActors.isEmpty)
              Future(MetricDropped(db, namespace, metric))
            else {
              (schemaCoordinator ? DeleteSchema(db, namespace, metric))
                .mapTo[SchemaDeleted]
                .flatMap(_ => broadcastMessage(msg))
            }
          case WriteToCommitLogFailed(_, _, _, _, reason) =>
            log.error(s"Failed to write to commit-log for: $msg with reason: $reason")
            context.system.terminate()
        }
        .pipeTo(sender())
    case Restore(path: String) =>
      log.info("restoring dump at path {}", path)
      val tmpPath = s"/tmp/nsdbDump/${UUID.randomUUID().toString}"

      sender ! Restored(path)

      FileUtils.unzip(path, tmpPath)
      val dbs = FileUtils.getSubDirs(tmpPath)
      dbs.foreach { db =>
        val namespaces = FileUtils.getSubDirs(db)
        namespaces.foreach { namespace =>
          val metrics = FileUtils.getSubDirs(namespace)
          metrics.foreach { metric =>
            val schemaIndex = new SchemaIndex(new MMapDirectory(Paths.get(metric.getAbsolutePath, "schemas")))
            schemaIndex.getSchema(metric.getName).foreach { schema =>
              log.debug("restoring metric {}", metric.getName)
              val metricsIndex = new TimeSeriesIndex(new MMapDirectory(metric.toPath))
              val minTimestamp = metricsIndex
                .query(schema,
                       new MatchAllDocsQuery(),
                       Seq.empty,
                       1,
                       Some(new Sort(new SortField("timestamp", SortField.Type.LONG, false))))(identity)
                .head
                .timestamp
              val maxTimestamp = metricsIndex
                .query(schema,
                       new MatchAllDocsQuery(),
                       Seq.empty,
                       1,
                       Some(new Sort(new SortField("timestamp", SortField.Type.LONG, true))))(identity)
                .head
                .timestamp
              var currentTimestamp = minTimestamp
              var upBound          = currentTimestamp + shardingInterval.toMillis

              while (currentTimestamp <= maxTimestamp) {

                val cluster  = Cluster(context.system)
                val nodeName = createNodeName(cluster.selfMember)
                val loc      = Location(metric.getName, nodeName, currentTimestamp, upBound)

                log.debug(s"restoring dump from metric ${metric.getName} and location $loc")

                metadataCoordinator ! (AddLocation(db.getName, namespace.getName, loc), ActorRef.noSender)
                metricsIndex
                  .query(schema,
                         LongPoint.newRangeQuery("timestamp", currentTimestamp, upBound),
                         Seq.empty,
                         Int.MaxValue,
                         None) { bit =>
                    updateSchema(db.getName, namespace.getName, metric.getName, bit) { _ =>
                      accumulateRecord(db.getName, namespace.getName, metric.getName, bit, loc)
                    }
                  }
                currentTimestamp = upBound
                upBound = currentTimestamp + shardingInterval.toMillis
                System.gc()
                System.runFinalization()
                log.info("path {} restored", path)
                sender() ! Restored(path)
              }
            }
          }
        }
      }
    case CreateDump(destPath, targets) =>
      log.info(s"Starting dump with destination : $destPath and targets : ${targets.mkString(",")}")

      val basePath       = context.system.settings.config.getString("nsdb.index.base-path")
      val dumpIdentifier = UUID.randomUUID().toString
      val tmpPath        = s"/tmp/nsdbDump/$dumpIdentifier"

      sender ! DumpCreated(destPath)

      targets
        .groupBy(_.db)
        .foreach {
          case (db, dbTargets) =>
            val namespaces = dbTargets.map(_.namespace)
            namespaces.foreach { namespace =>
              FileUtils
                .getSubDirs(Paths.get(basePath, db, namespace, "shards").toFile)
                .groupBy(_.getName.split("_").toList.head)
                .foreach {
                  case (metricName, dirNames) =>
                    (schemaCoordinator ? GetSchema(db, namespace, metricName))
                      .foreach {
                        case SchemaGot(_, _, _, Some(schema)) =>
                          val schemasDir =
                            new MMapDirectory(Paths.get(basePath, db, namespace, "schemas", metricName))
                          val schemaIndex  = new SchemaIndex(schemasDir)
                          val schemaWriter = schemaIndex.getWriter
                          schemaIndex.write(schema)(schemaWriter)
                          schemaWriter.close()

                          val dumpMetricIndex =
                            new TimeSeriesIndex(new MMapDirectory(Paths.get(tmpPath, db, namespace, metricName)))
                          dirNames.foreach { dirMetric =>
                            val shardWriter: IndexWriter = dumpMetricIndex.getWriter
                            val shardsDir =
                              new MMapDirectory(Paths.get(basePath, db, namespace, "shards", dirMetric.getName))
                            val shardIndex = new TimeSeriesIndex(shardsDir)
                            shardIndex.all(schema, bit => dumpMetricIndex.write(bit)(shardWriter))
                            shardWriter.close()
                            System.gc()
                            System.runFinalization()
                          }
                        case _ => log.error("No schema found for metric {}", metricName)
                      }
                }
            }
        }

      ZipUtil.pack(Paths.get(tmpPath).toFile, new File(s"$destPath/$dumpIdentifier.zip"))
      ApacheFileUtils.deleteDirectory(Paths.get(tmpPath).toFile)
      log.info(s"Dump with identifier $dumpIdentifier completed")

    case msg => log.info(s"Receive Unhandled message $msg")
  }
}
