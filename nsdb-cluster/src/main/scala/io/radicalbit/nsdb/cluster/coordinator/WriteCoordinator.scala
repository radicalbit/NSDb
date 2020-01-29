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

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props, Stash}
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.PubSubTopics.{COORDINATORS_TOPIC, NODE_GUARDIANS_TOPIC}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.{
  AddRecordToLocation,
  ExecuteDeleteStatementInternalInLocations
}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{
  GetLocations,
  GetWriteLocations
}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsGot
import io.radicalbit.nsdb.cluster.coordinator.WriteCoordinator._
import io.radicalbit.nsdb.cluster.util.ErrorManagementUtils._
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.index.DirectorySupport
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object WriteCoordinator {

  def props(metadataCoordinator: ActorRef, schemaCoordinator: ActorRef, mediator: ActorRef): Props =
    Props(new WriteCoordinator(metadataCoordinator, schemaCoordinator, mediator))

//  case class ExecuteRestoreMetadata(path: String)
//  case class MetadataRestored(path: String)

  case class AckPendingMetric(db: String, namespace: String, metric: String)
}

/**
  * Actor that receives every write (or delete) request and coordinates them among internal data storage.
  * @param metadataCoordinator  [[MetadataCoordinator]] the metadata coordinator.
  * @param schemaCoordinator   [[SchemaCoordinator]] the namespace schema actor.
  */
class WriteCoordinator(metadataCoordinator: ActorRef, schemaCoordinator: ActorRef, mediator: ActorRef)
    extends ActorPathLogging
    with DirectorySupport
    with NsdbPerfLogger
    with Stash {

  import akka.pattern.ask

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  log.info("WriteCoordinator is ready.")

  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  lazy val consistencyLevel: Int =
    context.system.settings.config.getInt("nsdb.cluster.consistency-level")

  private val metricsDataActors: mutable.Map[String, ActorRef]     = mutable.Map.empty
  private val commitLogCoordinators: mutable.Map[String, ActorRef] = mutable.Map.empty
  private val publishers: mutable.Map[String, ActorRef]            = mutable.Map.empty

  /**
    * This mutable state is aimed to store metric for which the actor is waiting for CommitLog ack
    * If a metric, identified according to its "coordinates" in [[AckPendingMetric]], is contained in this Set
    * the coming request for the latter will be stashed until CL acks are received.
    */
  private val ackPendingMetrics: mutable.Set[AckPendingMetric] = mutable.Set.empty

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
                             action: CommitLogAction,
                             location: Location): Future[CommitLogResponse] = {
    commitLogCoordinators.get(nodeName) match {
      case Some(commitLogCoordinator) =>
        (commitLogCoordinator ? WriteToCommitLog(db = db,
                                                 namespace = namespace,
                                                 metric = metric,
                                                 ts = ts,
                                                 action = action,
                                                 location)).mapTo[CommitLogResponse]
      case None =>
        Future(
          WriteToCommitLogFailed(db, namespace, ts, metric, s"Commit log not existing for requested node: $nodeName "))
    }
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
          Future(RecordRejected(db, namespace, metric, bit, Location.empty, errs, System.currentTimeMillis()))
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
        Future(
          RecordRejected(db,
                         namespace,
                         metric,
                         bit,
                         Location.empty,
                         List(s"no location found for bit $bit"),
                         System.currentTimeMillis()))
    }

  /**
    * Enqueues the bit into an internal structure. The real write is performed afterwards.
    * @param db the db.
    * @param namespace the namespace.
    * @param metric the metric.
    * @param bit the bit.
    * @param location the location to write the bit in.
    */
  def accumulateRecord(db: String,
                       namespace: String,
                       metric: String,
                       bit: Bit,
                       location: Location): Future[WriteCoordinatorResponse] =
    metricsDataActors.get(location.node) match {
      case Some(actor) =>
        (actor ? AddRecordToLocation(db, namespace, bit, location)).map {
          case msg: RecordAdded    => msg
          case msg: RecordRejected => msg
          case _ =>
            RecordRejected(db,
                           namespace,
                           metric,
                           bit,
                           location,
                           List("unknown response from metrics Actor"),
                           System.currentTimeMillis())
        }
      case None =>
        log.error(s"no data actor for node ${location.node}")
        Future(
          RecordRejected(db,
                         namespace,
                         metric,
                         bit,
                         location,
                         List(s"no data actor for node ${location.node}"),
                         System.currentTimeMillis()))
    }

  /**
    * Given a sequence of [[CommitLogResponse]] (may be successful or failed both) applies the passed function
    * ,if the responses are all successful. Otherwise, it returns a message of failure [[RecordRejected]].
    * The argument function maps incoming successful requests in a [[WriteCoordinatorResponse]]
    * that can be [[RecordRejected]] in case of failure  or [[InputMapped]] in case of success after fn application.
    *
    * In case of commitLog failed responses it performs compensation for correctly written entities.
    *
    * @param responses [[Seq]] of commit log responses
    * @param db database name
    * @param namespace namespace name
    * @param metric metric name
    * @param bit [[Bit]] being written
    * @param schema [[Bit]] schema
    * @param fn function to be applied in case of successful responses
    * @return a [[WriteCoordinatorResponse]]
    */
  def handleCommitLogCoordinatorResponses(responses: Seq[CommitLogResponse],
                                          db: String,
                                          namespace: String,
                                          metric: String,
                                          bit: Bit,
                                          schema: Schema)(
      fn: List[WriteToCommitLogSucceeded] => Future[WriteCoordinatorResponse]): Future[WriteCoordinatorResponse] = {
    val (succeedResponses: List[WriteToCommitLogSucceeded], failedResponses: List[WriteToCommitLogFailed]) =
      partitionResponses[WriteToCommitLogSucceeded, WriteToCommitLogFailed](responses)

    if (succeedResponses.size == responses.size) {
      fn(succeedResponses)
    } else {
      //Reverting successful writes committing Rejection Entry
      succeedResponses.foreach { response =>
        writeCommitLog(db,
                       namespace,
                       response.ts,
                       metric,
                       response.location.node,
                       RejectedEntryAction(bit),
                       response.location)
      }
      Future(
        RecordRejected(
          db,
          namespace,
          metric,
          bit,
          Location(metric, "", 0, 0),
          List(s"Error in CommitLog write request with reasons: ${failedResponses.map(_.reason)}"),
          System.currentTimeMillis()
        ))
    }
  }

  def handleAccumulatorResponses(responses: Seq[WriteCoordinatorResponse],
                                 db: String,
                                 namespace: String,
                                 metric: String,
                                 bit: Bit,
                                 schema: Schema,
                                 publish: Boolean): Future[WriteCoordinatorResponse] = {
    val (succeedResponses: List[RecordAdded], _: List[RecordRejected]) =
      partitionResponses[RecordAdded, RecordRejected](responses)

    if (succeedResponses.size == responses.size) {
      unstashAll()
      ackPendingMetrics -= AckPendingMetric(db, namespace, metric)
      if (publish)
        publishers.foreach {
          case (_, publisherActor) =>
            publisherActor ! PublishRecord(db, namespace, metric, bit, schema)
        }

      val accumulationResult = Future
        .sequence {
          responses.map { accumulatorResponse =>
            writeCommitLog(db,
                           namespace,
                           accumulatorResponse.timestamp,
                           metric,
                           accumulatorResponse.location.node,
                           AccumulatedEntryAction(bit),
                           accumulatorResponse.location)
          }
        }
        .flatMap { responses =>
          handleCommitLogCoordinatorResponses(responses, db, namespace, metric, bit, schema)(_ =>
            Future.successful(InputMapped(db, namespace, metric, bit)))
        }

      accumulationResult
    } else {
      // case in which the accumulation on some node failed
      // we need to delete the accumulated bits on the successful node commit in these nodes a RejectedEntry
      succeedResponses.foreach { response =>
        metricsDataActors.get(response.location.node) match {
          case Some(mda) => mda ! DeleteRecordFromShard(db, namespace, response.location, bit)
          case None      =>
        }
      }

      val commitLogRejection = Future
        .sequence {
          responses.map { accumulatorResponse =>
            writeCommitLog(db,
                           namespace,
                           accumulatorResponse.timestamp,
                           metric,
                           accumulatorResponse.location.node,
                           RejectedEntryAction(bit),
                           accumulatorResponse.location)
          }
        }
        .flatMap { responses =>
          handleCommitLogCoordinatorResponses(responses, db, namespace, metric, bit, schema)(
            res =>
              Future.successful(
                RecordRejected(db,
                               namespace,
                               metric,
                               bit,
                               Location(metric, "", 0, 0),
                               List(""),
                               System.currentTimeMillis())))
        }

      ackPendingMetrics -= AckPendingMetric(db, namespace, metric)
      commitLogRejection
    }
  }

  def writeOperation(locations: Seq[Location],
                     db: String,
                     namespace: String,
                     metric: String,
                     timestamp: Long,
                     bit: Bit,
                     schema: Schema,
                     publish: Boolean = true): Future[WriteCoordinatorResponse] = {
    val commitLogResponses: Future[Seq[CommitLogResponse]] =
      Future
        .sequence(locations.map { loc =>
          writeCommitLog(db, namespace, timestamp, metric, loc.node, ReceivedEntryAction(bit), loc)
        })

    commitLogResponses
      .flatMap { results =>
        handleCommitLogCoordinatorResponses(results, db, namespace, metric, bit, schema) { succeedResponses =>
          Future
            .sequence(succeedResponses.map { commitLogSuccess =>
              accumulateRecord(db, namespace, metric, bit, commitLogSuccess.location)
            })
            .flatMap { results =>
              handleAccumulatorResponses(results, db, namespace, metric, bit, schema, publish)
            }
        }
      }
  }

  override def preStart(): Unit = {
    mediator ! Subscribe(COORDINATORS_TOPIC, self)

    val interval = FiniteDuration(
      context.system.settings.config.getDuration("nsdb.publisher.scheduler.interval", TimeUnit.SECONDS),
      TimeUnit.SECONDS)

    context.system.scheduler.schedule(FiniteDuration(0, "ms"), interval) {
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetMetricsDataActors)
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetCommitLogCoordinators)
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetPublishers)
      log.debug("WriteCoordinator data actor : {}", metricsDataActors.size)
      log.debug("WriteCoordinator commit log  actor : {}", commitLogCoordinators.size)
    }
  }

  override def receive: Receive = {
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
    case SubscribePublisher(actor: ActorRef, nodeName) =>
      if (!publishers.get(nodeName).contains(actor)) {
        publishers += (nodeName -> actor)
        log.info(s"subscribed publisher actor for node $nodeName")
      }
      sender() ! PublisherSubscribed(actor, nodeName)

    case UnsubscribeMetricsDataActor(nodeName) =>
      metricsDataActors -= nodeName
      log.info(s"unsubscribed data actor for node $nodeName")
      sender() ! MetricsDataActorUnSubscribed(nodeName)
    case UnSubscribeCommitLogCoordinator(nodeName) =>
      commitLogCoordinators -= nodeName
      log.info(s"unsubscribed commit log actor for node $nodeName")
      sender() ! CommitLogCoordinatorUnSubscribed(nodeName)
    case UnSubscribePublisher(nodeName) =>
      publishers -= nodeName
      log.info(s"unsubscribed publisher actor for node $nodeName")
      sender() ! PublisherUnSubscribed(nodeName)
    case GetConnectedDataNodes =>
      sender ! ConnectedDataNodesGot(metricsDataActors.keys.toSeq)
    case MapInput(_, db, namespace, metric, _) if ackPendingMetrics.contains(AckPendingMetric(db, namespace, metric)) =>
      // Case covering request for a metric waiting for commit-log ack
      log.debug(s"Stashed message due to async ack from commit-log for metric $metric")
      stash()
    case MapInput(ts, db, namespace, metric, bit)
        if !ackPendingMetrics.contains(AckPendingMetric(db, namespace, metric)) =>
      // Case covering request for a metric not waiting for commit-log ack
      val startTime = System.currentTimeMillis()
      log.debug("Received a write request for (ts: {}, metric: {}, bit : {})", ts, metric, bit)
      getMetadataLocations(db, namespace, metric, bit, bit.timestamp) { locations =>
        updateSchema(db, namespace, metric, bit) { schema =>
          val consistentLocations = locations.take(consistencyLevel)
          val eventualLocations   = locations.diff(consistentLocations)

          val accumulatedResponses = writeOperation(consistentLocations, db, namespace, metric, startTime, bit, schema)

          // Send write requests for eventual consistent location iff consistent locations had accumulated the data
          accumulatedResponses.flatMap {
            case _: InputMapped =>
              writeOperation(eventualLocations, db, namespace, metric, startTime, bit, schema, publish = false)
            case r: RecordRejected =>
              // It does nothing for responses not included in consistency level
              log.error(
                s"Eventual writes are not performed due to error during consistent write operations ${r.reasons.mkString("")}")
              Future(r)
          }

          accumulatedResponses
        }
      }.pipeToWithEffect(sender()) { _ =>
        if (perfLogger.isDebugEnabled)
          perfLogger.debug("End write request in {} millis", System.currentTimeMillis() - startTime)
      }
    case msg @ DeleteNamespace(db, namespace) =>
      val chain = for {
        metrics <- (metadataCoordinator ? GetMetrics(db, namespace)).mapTo[MetricsGot].map(_.metrics)
        locations <- Future
          .sequence {
            metrics.map(metric =>
              (metadataCoordinator ? GetLocations(db, namespace, metric)).mapTo[LocationsGot].map(_.locations))
          }
          .map(_.flatten)
        commitLogResponses <- Future.sequence {
          locations
            .collect {
              case location if commitLogCoordinators.get(location.node).isDefined =>
                (location, commitLogCoordinators(location.node))
            }
            .map {
              case (location, actor) =>
                (actor ? WriteToCommitLog(db = db,
                                          namespace = namespace,
                                          metric = location.metric,
                                          ts = System.currentTimeMillis(),
                                          action = DeleteNamespaceAction(),
                                          location)).mapTo[CommitLogResponse]
            }
        }
        _ <- {
          val (_, errors) = partitionResponses[WriteToCommitLogSucceeded, WriteToCommitLogFailed](commitLogResponses)
          if (errors.nonEmpty) {
            log.error(s"Failed to write to commit-log for: $msg with reason: ${errors.map(_.reason).mkString("")}")
            context.system.terminate()
          }
          broadcastMessage(msg)
        }
        _ <- metadataCoordinator ? msg
        _ <- schemaCoordinator ? msg
      } yield NamespaceDeleted(db, namespace)

      chain.pipeTo(sender())
    case msg @ ExecuteDeleteStatement(statement @ DeleteSQLStatement(db, namespace, metric, _)) =>
      //FIXME add cluster aware deletion
      writeCommitLog(
        db,
        namespace,
        System.currentTimeMillis(),
        metric,
        commitLogCoordinators.keys.head,
        DeleteAction(statement),
        Location(metric, commitLogCoordinators.keys.head, 0, 0)
      ).flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _, _) =>
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
      val chain = for {
        locations <- (metadataCoordinator ? GetLocations(db, namespace, metric)).mapTo[LocationsGot].map(_.locations)
        commitLogResponses <- Future.sequence {
          locations
            .collect {
              case location if commitLogCoordinators.get(location.node).isDefined =>
                (location, commitLogCoordinators(location.node))
            }
            .map {
              case (location, actor) =>
                (actor ? WriteToCommitLog(db = db,
                                          namespace = namespace,
                                          metric = metric,
                                          ts = System.currentTimeMillis(),
                                          action = DeleteMetricAction(),
                                          location)).mapTo[CommitLogResponse]
            }
        }
        _ <- {
          val (_, errors) = partitionResponses[WriteToCommitLogSucceeded, WriteToCommitLogFailed](commitLogResponses)
          if (errors.nonEmpty) {
            log.error(s"Failed to write to commit-log for: $msg with reason: ${errors.map(_.reason).mkString("")}")
            context.system.terminate()
          }
          broadcastMessage(DropMetricWithLocations(db, namespace, metric, locations)).mapTo[MetricDropped]
        }
        _ <- metadataCoordinator ? DropMetric(db, namespace, metric)
        _ <- (schemaCoordinator ? DeleteSchema(db, namespace, metric)).mapTo[SchemaDeleted]
      } yield MetricDropped(db, namespace, metric)
      chain.pipeTo(sender())
//    case Restore(path: String) =>
//      log.info("restoring dump at path {}", path)
//      val tmpPath = s"/tmp/nsdbDump/${UUID.randomUUID().toString}"
//
//      sender ! Restored(path)
//
//      FileUtils.unzip(path, tmpPath)
//      val dbs = FileUtils.getSubDirs(tmpPath)
//      dbs.foreach { db =>
//        val namespaces = FileUtils.getSubDirs(db)
//        namespaces.foreach { namespace =>
//          val metrics = FileUtils.getSubDirs(namespace)
//          metrics.foreach { metric =>
//            val schemaIndex = new SchemaIndex(createMmapDirectory(Paths.get(metric.getAbsolutePath, "schemas")))
//            schemaIndex.getSchema(metric.getName).foreach { schema =>
//              log.debug("restoring metric {}", metric.getName)
//              val metricsIndex = new TimeSeriesIndex(createMmapDirectory(metric.toPath))
//              val minTimestamp = metricsIndex
//                .query(schema,
//                       new MatchAllDocsQuery(),
//                       Seq.empty,
//                       1,
//                       Some(new Sort(new SortField("timestamp", SortField.Type.LONG, false))))(identity)
//                .head
//                .timestamp
//              val maxTimestamp = metricsIndex
//                .query(schema,
//                       new MatchAllDocsQuery(),
//                       Seq.empty,
//                       1,
//                       Some(new Sort(new SortField("timestamp", SortField.Type.LONG, true))))(identity)
//                .head
//                .timestamp
//              var currentTimestamp = minTimestamp
//              var upBound          = currentTimestamp + shardingInterval.toMillis
//
//              while (currentTimestamp <= maxTimestamp) {
//
//                val cluster  = Cluster(context.system)
//                val nodeName = createNodeName(cluster.selfMember)
//                val loc      = Location(metric.getName, nodeName, currentTimestamp, upBound)
//
//                log.debug(s"restoring dump from metric ${metric.getName} and location $loc")
//
//                metadataCoordinator ! (AddLocation(db.getName, namespace.getName, loc), ActorRef.noSender)
//                metricsIndex
//                  .query(schema,
//                         LongPoint.newRangeQuery("timestamp", currentTimestamp, upBound),
//                         Seq.empty,
//                         Int.MaxValue,
//                         None) { bit =>
//                    updateSchema(db.getName, namespace.getName, metric.getName, bit) { _ =>
//                      accumulateRecord(db.getName, namespace.getName, metric.getName, bit, loc)
//                    }
//                  }
//                currentTimestamp = upBound
//                upBound = currentTimestamp + shardingInterval.toMillis
//                System.gc()
//                System.runFinalization()
//                log.info("path {} restored", path)
//                sender() ! Restored(path)
//              }
//            }
//          }
//        }
//      }
//    case CreateDump(destPath, targets) =>
//      log.info(s"Starting dump with destination : $destPath and targets : ${targets.mkString(",")}")
//
//      val basePath       = context.system.settings.config.getString(StorageIndexPath)
//      val dumpIdentifier = UUID.randomUUID().toString
//      val tmpPath        = s"/tmp/nsdbDump/$dumpIdentifier"
//
//      sender ! DumpCreated(destPath)
//
//      targets
//        .groupBy(_.db)
//        .foreach {
//          case (db, dbTargets) =>
//            val namespaces = dbTargets.map(_.namespace)
//            namespaces.foreach { namespace =>
//              FileUtils
//                .getSubDirs(Paths.get(basePath, db, namespace, "shards").toFile)
//                .groupBy(_.getName.split("_").toList.head)
//                .foreach {
//                  case (metricName, dirNames) =>
//                    (schemaCoordinator ? GetSchema(db, namespace, metricName))
//                      .foreach {
//                        case SchemaGot(_, _, _, Some(schema)) =>
//                          val schemasDir =
//                            createMmapDirectory(Paths.get(basePath, db, namespace, "schemas", metricName))
//                          val schemaIndex  = new SchemaIndex(schemasDir)
//                          val schemaWriter = schemaIndex.getWriter
//                          schemaIndex.write(schema)(schemaWriter)
//                          schemaWriter.close()
//
//                          val dumpMetricIndex =
//                            new TimeSeriesIndex(createMmapDirectory(Paths.get(tmpPath, db, namespace, metricName)))
//                          dirNames.foreach { dirMetric =>
//                            val shardWriter: IndexWriter = dumpMetricIndex.getWriter
//                            val shardsDir =
//                              createMmapDirectory(Paths.get(basePath, db, namespace, "shards", dirMetric.getName))
//                            val shardIndex = new TimeSeriesIndex(shardsDir)
//                            shardIndex.all(schema, bit => dumpMetricIndex.write(bit)(shardWriter))
//                            shardWriter.close()
//                            System.gc()
//                            System.runFinalization()
//                          }
//                        case _ => log.error("No schema found for metric {}", metricName)
//                      }
//                }
//            }
//        }
//
//      ZipUtil.pack(Paths.get(tmpPath).toFile, new File(s"$destPath/$dumpIdentifier.zip"))
//      ApacheFileUtils.deleteDirectory(Paths.get(tmpPath).toFile)
//      log.info(s"Dump with identifier $dumpIdentifier completed")

//    case msg @ Migrate(inputPath) =>
//      log.info("starting migrate at path {}", inputPath)
//
//      sender ! MigrationStarted(inputPath)
//
//      for {
//        _              <- metadataCoordinator ? msg
//        schemaMigrated <- (schemaCoordinator ? msg).mapTo[SchemaMigrated]
//        accumulated <- {
//
//          val schemaMap = schemaMigrated.schemas.toMap
//
//          Future.sequence(
//            FileUtils
//              .getSubDirs(inputPath)
//              .flatMap { db =>
//                FileUtils.getSubDirs(db).toList.map {
//                  namespace =>
//                    FileUtils.getSubDirs(Paths.get(namespace.getAbsolutePath, "shards")).flatMap {
//                      shard =>
//                        val metric   = shard.getName.split("_").headOption
//                        val shardDir = createMmapDirectory(Paths.get(shard.getAbsolutePath))
//                        new IndexUpgrader(shardDir).upgrade()
//
//                        val shardIndex = new TimeSeriesIndex(shardDir)
//
//                        val schemaOpt =
//                          metric.flatMap(m => schemaMap.get(Coordinates(db.getName, namespace.getName, m)))
//
//                        schemaOpt
//                          .map(s => shardIndex.all(s, b => (s.metric, b)))
//                          .getOrElse(Seq.empty)
//                          .map {
//                            case (metric, bit) =>
//                              self ? MapInput(bit.timestamp, db.getName, namespace.getName, metric, bit)
//                          }
//                    }
//                }
//              }
//              .flatten)
//        }
//      } yield accumulated

    case msg => log.info(s"Receive Unhandled message $msg")
  }
}
