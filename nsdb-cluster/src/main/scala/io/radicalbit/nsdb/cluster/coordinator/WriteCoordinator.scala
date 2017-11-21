package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands.GetWriteLocation
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events.LocationGot
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

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
    with ActorLogging {

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

  def shardBehaviour: Receive = {
    case SubscribeNamespaceDataActor(actor: ActorRef, Some(nodeName)) =>
      namespaces += (nodeName -> actor)
      sender() ! NamespaceDataActorSubscribed(actor, Some(nodeName))
    case SubscribeNamespaceDataActor(actor: ActorRef, None) =>
      sender() ! NamespaceDataActorSubscriptionFailed(actor, None, "cannot subscribe ")
    case MapInput(ts, db, namespace, metric, bit) =>
      log.debug("Received a write request for (ts: {}, metric: {}, bit : {})", ts, metric, bit)
      (namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, metric, bit))
        .flatMap {
          case SchemaUpdated(_, _, _) =>
            log.debug("Valid schema for the metric {} and the bit {}", metric, bit)
            val commitLogFuture: Future[WroteToCommitLogAck] =
              if (commitLogService.isDefined)
                (commitLogService.get ? CommitLogService.Insert(ts = ts, metric = metric, record = bit))
                  .mapTo[WroteToCommitLogAck]
              else Future.successful(WroteToCommitLogAck(ts, metric, bit))
            commitLogFuture
              .flatMap(ack => {
                publisherActor ! InputMapped(db, namespace, metric, bit)
                (metadataCoordinator ? GetWriteLocation(db, namespace, metric, ack.ts)).mapTo[LocationGot].flatMap {
                  case LocationGot(_, _, _, Some(Location(_, node, _, _, _))) =>
                    namespaces.get(node) match {
                      case Some(actor) => (actor ? AddRecord(db, namespace, ack.metric, ack.bit)).mapTo[RecordAdded]
                      case None =>
                        Future(RecordRejected(db, namespace, metric, bit, List(s"no data actor for node $node")))
                    }
                  case _ => Future(RecordRejected(db, namespace, metric, bit, List(s"no location found for bit $bit")))
                }
                Future(RecordAdded(db, namespace, metric, bit))
              })
              .map(r => InputMapped(db, namespace, metric, r.record))
          case UpdateSchemaFailed(_, _, _, errs) =>
            log.error("Invalid schema for the metric {} and the bit {}. Error are {}.",
                      metric,
                      bit,
                      errs.mkString(","))
            Future(RecordRejected(db, namespace, metric, bit, errs))
        }
        .pipeTo(sender())
  }

  def init: Receive = {
    case SubscribeNamespaceDataActor(actor: ActorRef, _) =>
      context.become(subscribed(actor))
      sender() ! NamespaceDataActorSubscribed(actor)
  }

  def subscribed(namespaceDataActor: ActorRef): Receive = {
    case MapInput(ts, db, namespace, metric, bit) =>
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
                (namespaceDataActor ? AddRecord(db, namespace, ack.metric, ack.bit)).mapTo[RecordAdded]
              })
              .map(r => InputMapped(db, namespace, metric, r.record))
          case UpdateSchemaFailed(_, _, _, errs) =>
            log.error("Invalid schema for the metric {} and the bit {}. Error are {}.",
                      metric,
                      bit,
                      errs.mkString(","))
            Future(RecordRejected(db, namespace, metric, bit, errs))
        }
        .pipeTo(sender())
    case msg @ DeleteNamespace(_, _) =>
      (namespaceDataActor ? msg)
        .mapTo[NamespaceDeleted]
        .flatMap(_ => namespaceSchemaActor ? msg)
        .mapTo[NamespaceDeleted]
        .pipeTo(sender())
    case ExecuteDeleteStatement(statement) =>
      log.debug(s"executing $statement")
      (namespaceSchemaActor ? GetSchema(statement.db, statement.namespace, statement.metric))
        .flatMap {
          case SchemaGot(_, _, _, Some(schema)) =>
            namespaceDataActor ? ExecuteDeleteStatementInternal(statement, schema)
          case _ => Future(SelectStatementFailed(s"Metric ${statement.metric} does not exist "))
        }
        .pipeTo(sender())
    case msg @ DropMetric(_, _, _) =>
      namespaceDataActor forward msg
  }
}

trait JournalWriter

class AsyncJournalWriter {}
