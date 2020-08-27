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
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.cluster.PubSubTopics.{COORDINATORS_TOPIC, NODE_GUARDIANS_TOPIC}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.{
  AddRecordToLocation,
  ExecuteDeleteStatementInternalInLocations
}
import io.radicalbit.nsdb.cluster.actor.SequentialFutureProcessing
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{GetLocations, GetWriteLocations}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.logic.WriteConfig
import io.radicalbit.nsdb.util.ErrorManagementUtils._
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.configuration.NSDbConfig
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.index.{DirectorySupport, StorageStrategy}
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object WriteCoordinator {

  def props(metadataCoordinator: ActorRef, schemaCoordinator: ActorRef, mediator: ActorRef): Props =
    Props(new WriteCoordinator(metadataCoordinator, schemaCoordinator, mediator))

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
    with SequentialFutureProcessing
    with WriteConfig
    with Stash {

  import akka.pattern.ask

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  log.info("WriteCoordinator is ready.")

  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  override lazy val indexStorageStrategy: StorageStrategy =
    StorageStrategy.withValue(context.system.settings.config.getString(NSDbConfig.HighLevel.StorageStrategy))

  private val metricsDataActors: mutable.Map[String, ActorRef]     = mutable.Map.empty
  private val commitLogCoordinators: mutable.Map[String, ActorRef] = mutable.Map.empty
  private val publishers: mutable.Map[String, ActorRef]            = mutable.Map.empty

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
  def getWriteLocations(db: String, namespace: String, metric: String, bit: Bit, ts: Long)(
      op: Seq[Location] => Future[Any]): Future[Any] =
    (metadataCoordinator ? GetWriteLocations(db, namespace, metric, ts)).mapTo[GetWriteLocationsResponse].flatMap {
      case WriteLocationsGot(_, _, _, locations) =>
        log.debug(s"received locations for metric $metric, $locations")
        op(locations)
      case GetWriteLocationsBeyondRetention(_, _, _, _, retention) =>
        log.warning(
          s"bit $bit will not be written because its timestamp is beyond retention of $retention. Sending positive response.")
        Future(InputMapped(db, namespace, metric, bit))
      case GetWriteLocationsFailed(db, namespace, metric, timestamp, reason) =>
        val errorMessage = s"no location found for bit $bit:\n $reason"
        log.error(errorMessage)
        Future(RecordRejected(db, namespace, metric, bit, Location.empty, List(reason), timestamp))
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

      commitLogRejection
    }
  }

  def accumulateOperation(locations: Seq[Location],
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
    case MapInput(ts, db, namespace, metric, bit) =>
      val startTime = System.currentTimeMillis()
      log.debug("Received a write request for (ts: {}, metric: {}, bit : {})", ts, metric, bit)

      val writeOperation = getWriteLocations(db, namespace, metric, bit, bit.timestamp) { locations =>
        updateSchema(db, namespace, metric, bit) { schema =>
          accumulateOperation(locations, db, namespace, metric, startTime, bit, schema)
        }
      }

      val writeOperationEffect: Any => Unit = { _ =>
        if (perfLogger.isDebugEnabled)
          perfLogger.debug("End write request in {} millis", System.currentTimeMillis() - startTime)
      }

      writeProcessing match {
        case Parallel =>
          import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._
          writeOperation.pipeToWithEffect(sender()) { writeOperationEffect }
        case Serial =>
          sequential(writeOperation).pipeToWithEffect(sender()) { writeOperationEffect }
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

      sequential(chain).pipeTo(sender())
    case msg @ ExecuteDeleteStatement(statement @ DeleteSQLStatement(db, namespace, metric, _)) =>
      //FIXME add cluster aware deletion
      sequential {
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
                    Future(DeleteStatementFailed(db,
                                                 namespace,
                                                 metric,
                                                 s"No schema found for metric ${statement.metric}"),
                           MetricNotFound(metric))
                }
          case WriteToCommitLogFailed(_, _, _, _, reason) =>
            log.error(s"Failed to write to commit-log for: $msg with reason: $reason")
            context.system.terminate()
        }
      }.pipeTo(sender())
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
      sequential(chain).pipeTo(sender())
    case msg => log.info(s"Receive Unhandled message $msg")
  }
}
