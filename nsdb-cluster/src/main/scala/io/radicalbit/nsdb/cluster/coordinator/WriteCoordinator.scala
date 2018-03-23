package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands.{GetLocations, GetWriteLocation}
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events.{LocationGot, LocationsGot}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor.{
  AddRecordToLocation,
  ExecuteDeleteStatementInternalInLocations
}
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

class WriteCoordinator(commitLogCoordinator: Option[ActorRef],
                       metadataCoordinator: ActorRef,
                       namespaceSchemaActor: ActorRef,
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

  private def broadcastMessage(msg: Any) =
    Future
      .sequence(namespaces.values.toSeq.map(actor => actor ? msg))
      .map(_.head)

  private def commitLogFuture(db: String,
                              namespace: String,
                              ts: Long,
                              metric: String,
                              action: CommitLoggerAction): Future[CommitLogResponse] = {
    if (commitLogEnabled)
      (commitLogCoordinator.get ? WriteToCommitLog(db = db,
                                                   namespace = namespace,
                                                   metric = metric,
                                                   ts = ts,
                                                   action = action))
        .mapTo[CommitLogResponse]
    else Future.successful(WriteToCommitLogSucceeded(db = db, namespace = namespace, ts, metric))
  }

  def updateSchema(db: String, namespace: String, metric: String, bit: Bit)(f: Schema => Future[Any]): Future[Any] = {
    val timestamp = System.currentTimeMillis()
    (namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, metric, bit))
      .flatMap {
        case SchemaUpdated(_, _, _, schema) =>
          log.debug("Valid schema for the metric {} and the bit {}", metric, bit)
          f(schema)
        case UpdateSchemaFailed(_, _, _, errs) =>
          log.error("Invalid schema for the metric {} and the bit {}. Error are {}.", metric, bit, errs.mkString(","))
          commitLogFuture(db, namespace, timestamp, metric, RejectAction(bit)).map {
            case WriteToCommitLogSucceeded(_, _, _, _) =>
              RecordRejected(db, namespace, metric, bit, errs)
            case WriteToCommitLogFailed(_, _, _, _, reason) =>
              log.error(s"Failed to write to commit-log for RejectBit with reason: $reason")
              context.system.terminate()
          }
      }
  }

  def getMetadataLocation(db: String, namespace: String, metric: String, bit: Bit, ts: Long)(
      f: Location => Future[Any]): Future[Any] =
    (metadataCoordinator ? GetWriteLocation(db, namespace, metric, ts)).flatMap {
      case LocationGot(_, _, _, Some(loc)) =>
        log.debug(s"received location for metric $metric, $loc")
        f(loc)
      case _ =>
        log.error(s"no location found for bit $bit")
        Future(RecordRejected(db, namespace, metric, bit, List(s"no location found for bit $bit")))
    }

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
        commitLogFuture(db, namespace, bit.timestamp, metric, InsertAction(bit))
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
      commitLogFuture(db, namespace, System.currentTimeMillis(), "", DeleteNamespaceAction)
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (namespaces.isEmpty) {
              (namespaceSchemaActor ? msg).map(_ => NamespaceDeleted(db, namespace))
            } else
              (namespaceSchemaActor ? msg).flatMap(_ => broadcastMessage(msg))
          case WriteToCommitLogFailed(_, _, _, _, reason) =>
            log.error(s"Failed to write to commit-log for: $msg with reason: $reason")
            context.system.terminate()
        }
        .pipeTo(sender())
    case msg @ ExecuteDeleteStatement(statement @ DeleteSQLStatement(db, namespace, metric, _)) =>
      commitLogFuture(db, namespace, System.currentTimeMillis(), metric, DeleteAction(statement))
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (namespaces.isEmpty)
              Future(DeleteStatementExecuted(statement.db, statement.metric, statement.metric))
            else
              (namespaceSchemaActor ? GetSchema(statement.db, statement.namespace, statement.metric))
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
      val replyTo = sender()
      commitLogFuture(db, namespace, System.currentTimeMillis(), metric, DeleteMetricAction)
        .flatMap {
          case WriteToCommitLogSucceeded(_, _, _, _) =>
            if (namespaces.isEmpty)
              Future(MetricDropped(db, namespace, metric))
            else {
              (namespaceSchemaActor ? DeleteSchema(db, namespace, metric))
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
