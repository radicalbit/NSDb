package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.{
  AddRecordToLocation,
  ExecuteDeleteStatementInternalInLocations
}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{GetLocations, GetWriteLocation}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{LocationGot, LocationsGot}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.index.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._

import scala.collection.mutable
import scala.concurrent.Future

object WriteCoordinator {

  def props(commitLogCoordinator: Option[ActorRef],
            metadataCoordinator: ActorRef,
            namespaceSchemaActor: ActorRef,
            publisherActor: ActorRef): Props =
    Props(new WriteCoordinator(commitLogCoordinator, metadataCoordinator, namespaceSchemaActor, publisherActor))

}

/**
  * Actor that receives every write (or delete) request and coordinates them among internal data storage.
  * @param metadataCoordinator  [[MetadataCoordinator]] the metadata coordinator.
  * @param metricsSchemaActor [[io.radicalbit.nsdb.cluster.actor.MetricsSchemaActor]] the namespace schema actor.
  * @param commitLogCoordinator     [[CommitLogCoordinator]] the commit log coordinator.
  * @param publisherActor       [[io.radicalbit.nsdb.actors.PublisherActor]] the publisher actor.
  */
class WriteCoordinator(commitLogCoordinator: Option[ActorRef],
                       metadataCoordinator: ActorRef,
                       metricsSchemaActor: ActorRef,
                       publisherActor: ActorRef)
    extends Actor
    with ActorLogging
    with NsdbPerfLogger {

  import akka.pattern.ask

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  val commitLogEnabled: Boolean = context.system.settings.config.getBoolean("nsdb.commit-log.enabled")
  log.info("WriteCoordinator is ready.")
  if (!commitLogEnabled)
    log.info("Commit Log is disabled")

  lazy val sharding: Boolean          = context.system.settings.config.getBoolean("nsdb.sharding.enabled")
  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  private val namespaces: mutable.Map[String, ActorRef] = mutable.Map.empty

  /**
    * Performs an ask to every namespace actor subscribed.
    * @param msg the message to be sent.
    * @return the result of the broadcast.
    */
  private def broadcastMessage(msg: Any) =
    Future
      .sequence(namespaces.values.toSeq.map(actor => actor ? msg))
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
                             action: CommitLoggerAction): Future[CommitLogResponse] = {
    if (commitLogEnabled && commitLogCoordinator.isDefined)
      (commitLogCoordinator.get ? WriteToCommitLog(db = db,
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
  def updateSchema(db: String, namespace: String, metric: String, bit: Bit)(op: Schema => Future[Any]): Future[Any] = {
    val timestamp = System.currentTimeMillis()
    (metricsSchemaActor ? UpdateSchemaFromRecord(db, namespace, metric, bit))
      .flatMap {
        case SchemaUpdated(_, _, _, schema) =>
          log.debug("Valid schema for the metric {} and the bit {}", metric, bit)
          op(schema)
        case UpdateSchemaFailed(_, _, _, errs) =>
          log.error("Invalid schema for the metric {} and the bit {}. Error are {}.", metric, bit, errs.mkString(","))
          writeCommitLog(db, namespace, timestamp, metric, RejectAction(bit)).map {
            case WriteToCommitLogSucceeded(_, _, _, _) =>
              RecordRejected(db, namespace, metric, bit, errs)
            case WriteToCommitLogFailed(_, _, _, _, reason) =>
              log.error(s"Failed to write to commit-log for RejectBit with reason: $reason")
              context.system.terminate()
          }
      }
  }

  /**
    * Gets the metadata location to write the bit in.
    * @param db the db.
    * @param namespace the namespace.
    * @param metric the metric.
    * @param bit the bit.
    * @param ts the timestamp of the write operation.
    * @param op code executed in case of success.
    */
  def getMetadataLocation(db: String, namespace: String, metric: String, bit: Bit, ts: Long)(
      op: Location => Future[Any]): Future[Any] =
    (metadataCoordinator ? GetWriteLocation(db, namespace, metric, ts)).flatMap {
      case LocationGot(_, _, _, Some(loc)) =>
        log.debug(s"received location for metric $metric, $loc")
        op(loc)
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
    namespaces.get(location.node) match {
      case Some(actor) =>
        (actor ? AddRecordToLocation(db, namespace, bit, location)).map {
          case r: RecordAdded      => InputMapped(db, namespace, metric, r.record)
          case msg: RecordRejected => msg
          case _ =>
            RecordRejected(db, namespace, metric, bit, List("unknown response from NamespaceActor"))
        }
      case None =>
        log.error(s"no data actor for node ${location.node}")
        Future(RecordRejected(db, namespace, metric, bit, List(s"no data actor for node ${location.node}")))
    }

  override def receive: Receive = {
    case SubscribeNamespaceDataActor(actor: ActorRef, nodeName) =>
      namespaces += (nodeName -> actor)
      log.info(s"subscribed data actor for node $nodeName")
      sender() ! NamespaceDataActorSubscribed(actor, nodeName)
    case msg @ MapInput(ts, db, namespace, metric, bit) =>
      val startTime = System.currentTimeMillis()
      log.debug("Received a write request for (ts: {}, metric: {}, bit : {})", ts, metric, bit)
      updateSchema(db, namespace, metric, bit) { schema =>
        writeCommitLog(db, namespace, bit.timestamp, metric, InsertAction(bit))
          .flatMap {
            case WriteToCommitLogSucceeded(_, _, _, _) =>
              publisherActor ! PublishRecord(db, namespace, metric, bit, schema)
              getMetadataLocation(db, namespace, metric, bit, bit.timestamp) { loc =>
                accumulateRecord(db, namespace, metric, bit, loc)
              }
            case WriteToCommitLogFailed(_, _, _, _, reason) =>
              log.error(s"Failed to write to commit-log for: $msg with reason: $reason")
              context.system.terminate()
          }
      }.pipeToWithEffect(sender()) { _ =>
        if (perfLogger.isDebugEnabled)
          perfLogger.debug("End write request in {} millis", System.currentTimeMillis() - startTime)
      }
    case msg @ DeleteNamespace(db, namespace) =>
      writeCommitLog(db, namespace, System.currentTimeMillis(), "", DeleteNamespaceAction)
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (namespaces.isEmpty) {
              (metricsSchemaActor ? msg).map(_ => NamespaceDeleted(db, namespace))
            } else
              (metricsSchemaActor ? msg).flatMap(_ => broadcastMessage(msg))
          case WriteToCommitLogFailed(_, _, _, _, reason) =>
            log.error(s"Failed to write to commit-log for: $msg with reason: $reason")
            context.system.terminate()
        }
        .pipeTo(sender())
    case msg @ ExecuteDeleteStatement(statement @ DeleteSQLStatement(db, namespace, metric, _)) =>
      writeCommitLog(db, namespace, System.currentTimeMillis(), metric, DeleteAction(statement))
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (namespaces.isEmpty)
              Future(DeleteStatementExecuted(statement.db, statement.metric, statement.metric))
            else
              (metricsSchemaActor ? GetSchema(statement.db, statement.namespace, statement.metric))
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
      writeCommitLog(db, namespace, System.currentTimeMillis(), metric, DeleteMetricAction)
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (namespaces.isEmpty)
              Future(MetricDropped(db, namespace, metric))
            else {
              (metricsSchemaActor ? DeleteSchema(db, namespace, metric))
                .mapTo[SchemaDeleted]
                .flatMap(_ => broadcastMessage(msg))
            }
          case WriteToCommitLogFailed(_, _, _, _, reason) =>
            log.error(s"Failed to write to commit-log for: $msg with reason: $reason")
            context.system.terminate()
        }
        .pipeTo(sender())

    case msg => log.info(s"Receive Unhandled message $msg")

  }
}
