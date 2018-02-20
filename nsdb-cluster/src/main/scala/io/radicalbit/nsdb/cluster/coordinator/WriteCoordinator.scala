package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands.{GetLocations, GetWriteLocation}
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events.{LocationGot, LocationsGot}
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor.{
  AddRecordToLocation,
  ExecuteDeleteStatementInternalInLocations
}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._

import scala.collection.mutable
import scala.concurrent.Future

object WriteCoordinator {

  def props(metadataCoordinator: ActorRef,
            namespaceSchemaActor: ActorRef,
            commitLogService: Option[ActorRef],
            publisherActor: ActorRef): Props =
    Props(new WriteCoordinator(metadataCoordinator, namespaceSchemaActor, commitLogService, publisherActor))

}

class WriteCoordinator(metadataCoordinator: ActorRef,
                       namespaceSchemaActor: ActorRef,
                       commitLogService: Option[ActorRef],
                       publisherActor: ActorRef)
    extends Actor
    with ActorLogging
    with NsdbPerfLogger {

  import akka.pattern.ask

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  log.info("WriteCoordinator is ready.")
  if (commitLogService.isEmpty)
    log.info("Commit Log is disabled")

  lazy val sharding: Boolean          = context.system.settings.config.getBoolean("nsdb.sharding.enabled")
  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  private val namespaces: mutable.Map[String, ActorRef] = mutable.Map.empty

  override def receive: Receive = if (sharding) shardBehaviour else init

  private def broadcastMessage(msg: Any) =
    Future
      .sequence(namespaces.values.toSeq.map(actor => actor ? msg))
      .map(_.head)

  def shardBehaviour: Receive = {
    case SubscribeNamespaceDataActor(actor: ActorRef, Some(nodeName)) =>
      namespaces += (nodeName -> actor)
      log.info(s"subscribed data actor for node $nodeName")
      sender() ! NamespaceDataActorSubscribed(actor, Some(nodeName))
    case SubscribeNamespaceDataActor(actor: ActorRef, None) =>
      sender() ! NamespaceDataActorSubscriptionFailed(actor, None, "cannot subscribe ")
    case MapInput(ts, db, namespace, metric, bit) =>
      val startTime = System.currentTimeMillis()
      log.debug("Received a write request for (ts: {}, metric: {}, bit : {})", ts, metric, bit)
      (namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, metric, bit))
        .flatMap {
          case SchemaUpdated(_, _, _, schema) =>
            log.debug("Valid schema for the metric {} and the bit {}", metric, bit)
            val commitLogFuture: Future[WroteToCommitLogAck] =
              if (commitLogService.isDefined)
                (commitLogService.get ? CommitLogService.Insert(ts = ts, metric = metric, record = bit))
                  .mapTo[WroteToCommitLogAck]
              else Future.successful(WroteToCommitLogAck(ts, metric, bit))
            commitLogFuture
              .flatMap(ack => {
                publisherActor ! PublishRecord(db, namespace, metric, bit, schema)
                (metadataCoordinator ? GetWriteLocation(db, namespace, metric, ack.ts)).flatMap {
                  case LocationGot(_, _, _, Some(loc)) =>
                    log.debug(s"received location for metric $metric, $loc")
                    namespaces.get(loc.node) match {
                      case Some(actor) =>
                        (actor ? AddRecordToLocation(db, namespace, ack.metric, ack.bit, loc)).map {
                          case r: RecordAdded      => InputMapped(db, namespace, metric, r.record)
                          case msg: RecordRejected => msg
                          case _ =>
                            RecordRejected(db, namespace, metric, bit, List("unknown response from NamespaceActor"))
                        }
                      case None =>
                        log.debug(s"no data actor for node ${loc.node}")
                        Future(RecordRejected(db, namespace, metric, bit, List(s"no data actor for node ${loc.node}")))
                    }
                  case _ =>
                    log.debug(s"no location found for bit $bit")
                    Future(RecordRejected(db, namespace, metric, bit, List(s"no location found for bit $bit")))
                }
              })
          case UpdateSchemaFailed(_, _, _, errs) =>
            log.error("Invalid schema for the metric {} and the bit {}. Error are {}.",
                      metric,
                      bit,
                      errs.mkString(","))
            Future(RecordRejected(db, namespace, metric, bit, errs))
        }
        .pipeToWithEffect(sender()) { () =>
          if (perfLogger.isDebugEnabled)
            perfLogger.debug("End write request in {} millis", System.currentTimeMillis() - startTime)
        }
    case msg @ DeleteNamespace(db, namespace) =>
      if (namespaces.isEmpty)
        (namespaceSchemaActor ? msg).map(_ => NamespaceDeleted(db, namespace)).pipeTo(sender)
      else
        (namespaceSchemaActor ? msg).flatMap(_ => broadcastMessage(msg)).pipeTo(sender)
    case ExecuteDeleteStatement(statement @ DeleteSQLStatement(db, namespace, metric, _)) =>
      if (namespaces.isEmpty)
        sender() ! DeleteStatementExecuted(statement.db, statement.metric, statement.metric)
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
              Future(DeleteStatementFailed(db, namespace, metric, s"No schema found for metric ${statement.metric}"))
          }
          .pipeTo(sender())
    case msg @ DropMetric(db, namespace, metric) =>
      if (namespaces.isEmpty)
        sender() ! MetricDropped(db, namespace, metric)
      else {
        (namespaceSchemaActor ? DeleteSchema(db, namespace, metric))
          .mapTo[SchemaDeleted]
          .flatMap(_ => broadcastMessage(msg))
          .pipeTo(sender())
      }
  }

  def init: Receive = {
    case SubscribeNamespaceDataActor(actor: ActorRef, _) =>
      context.become(subscribed(actor))
      sender() ! NamespaceDataActorSubscribed(actor)
  }

  def subscribed(namespaceDataActor: ActorRef): Receive = {
    case MapInput(ts, db, namespace, metric, bit) =>
      val startTime = System.currentTimeMillis
      log.debug("Received a write request for (ts: {}, namespace: {},  metric: {}, bit : {})",
                ts,
                namespace,
                metric,
                bit)
      (namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, metric, bit))
        .flatMap {
          case SchemaUpdated(_, _, _, schema) =>
            log.debug("Valid schema for the metric {} and the bit {}", metric, bit)
            val commitLogFuture: Future[WroteToCommitLogAck] =
              if (commitLogService.isDefined)
                (commitLogService.get ? CommitLogService.Insert(ts = ts, metric = metric, record = bit))
                  .mapTo[WroteToCommitLogAck]
              else Future.successful(WroteToCommitLogAck(ts, metric, bit))
            commitLogFuture
              .flatMap(ack => {
                publisherActor ! PublishRecord(db, namespace, metric, bit, schema)
                namespaceDataActor ? AddRecord(db, namespace, ack.metric, ack.bit)
              })
              .map {
                case RecordAdded(_, _, _, record)        => InputMapped(db, namespace, metric, record)
                case RecordRejected(_, _, _, _, reasons) => RecordRejected(db, namespace, metric, bit, reasons)
                case _                                   => RecordRejected(db, namespace, metric, bit, List(s"unknown error while adding record $bit"))
              }
          case UpdateSchemaFailed(_, _, _, errs) =>
            log.error("Invalid schema for the metric {} and the bit {}. Error are {}.",
                      metric,
                      bit,
                      errs.mkString(","))
            Future(RecordRejected(db, namespace, metric, bit, errs))
          case t =>
            Future(RecordRejected(db, namespace, metric, bit, List("unknown error while updating schema")))
        }
        .recoverWith {
          case t => Future(RecordRejected(db, namespace, metric, bit, List(t.getMessage)))
        }
        .pipeToWithEffect(sender()) { () =>
          if (perfLogger.isDebugEnabled)
            perfLogger.debug("End write request in {} millis", System.currentTimeMillis - startTime)
        }
    case msg @ DeleteNamespace(_, _) =>
      (namespaceDataActor ? msg)
        .mapTo[NamespaceDeleted]
        .flatMap(_ => namespaceSchemaActor ? msg)
        .mapTo[NamespaceDeleted]
        .pipeTo(sender())
    case ExecuteDeleteStatement(statement @ DeleteSQLStatement(db, namespace, metric, _)) =>
      log.debug(s"executing $statement")
      (namespaceSchemaActor ? GetSchema(statement.db, statement.namespace, statement.metric))
        .flatMap {
          case SchemaGot(_, _, _, Some(schema)) =>
            namespaceDataActor ? ExecuteDeleteStatementInternal(statement, schema)
          case _ => Future(DeleteStatementFailed(db, namespace, metric, s"Metric ${statement.metric} does not exist "))
        }
        .pipeTo(sender())
    case msg @ DropMetric(db, namespace, metric) =>
      (namespaceSchemaActor ? DeleteSchema(db, namespace, metric))
        .mapTo[SchemaDeleted]
        .flatMap(_ => namespaceDataActor ? msg)
        .mapTo[MetricDropped]
        .pipeTo(sender)
  }
}
