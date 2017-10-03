package io.radicalbit.nsdb.coordinator

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.actors.NamespaceDataActor.commands.AddRecord
import io.radicalbit.nsdb.actors.NamespaceDataActor.events.{RecordAdded, RecordRejected}
import io.radicalbit.nsdb.actors.NamespaceSchemaActor.commands.UpdateSchemaFromRecord
import io.radicalbit.nsdb.actors.NamespaceSchemaActor.events.{SchemaUpdated, UpdateSchemaFailed}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.coordinator.WriteCoordinator._

import scala.concurrent.Future

object WriteCoordinator {

  def props(namespaceSchemaActor: ActorRef,
            commitLogService: ActorRef,
            namespaceDataActor: ActorRef,
            publisherActor: ActorRef): Props =
    Props(new WriteCoordinator(namespaceSchemaActor, commitLogService, namespaceDataActor, publisherActor))

  sealed trait WriteCoordinatorProtocol

  case class FlatInput(ts: Long, namespace: String, metric: String, data: Array[Byte]) extends WriteCoordinatorProtocol

  case class MapInput(ts: Long, namespace: String, metric: String, record: Bit) extends WriteCoordinatorProtocol
  case class InputMapped(namespace: String, metric: String, record: Bit)        extends WriteCoordinatorProtocol

  case class ExecuteDeleteStatement(statement: DeleteSQLStatement)
  case class DeleteStatementExecuted(namespace: String, metric: String)
  case class DeleteStatementFailed(namespace: String, metric: String, reason: String)

  case class DropMetric(namespace: String, metric: String)
  case class MetricDropped(namespace: String, metric: String)

  case class DeleteNamespace(namespace: String)
  case class NamespaceDeleted(namespace: String)
}

class WriteCoordinator(namespaceSchemaActor: ActorRef,
                       commitLogService: ActorRef,
                       namespaceDataActor: ActorRef,
                       publisherActor: ActorRef)
    extends Actor
    with ActorLogging {

  import akka.pattern.ask

  implicit val timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  log.info("WriteCoordinator is ready.")

  override def receive = {
    case WriteCoordinator.MapInput(ts, namespace, metric, record) =>
      log.debug("Received a write request for (ts: {}, metric: {}, record : {})", ts, metric, record)
      (namespaceSchemaActor ? UpdateSchemaFromRecord(namespace, metric, record))
        .flatMap {
          case SchemaUpdated(_, _) =>
            log.debug("Valid schema for the metric {} and the record {}", metric, record)
            (commitLogService ? CommitLogService.Insert(ts = ts, metric = metric, record = record))
              .mapTo[WroteToCommitLogAck]
              .flatMap(ack => {
                publisherActor ! InputMapped(namespace, metric, record)
                (namespaceDataActor ? AddRecord(namespace, ack.metric, ack.record)).mapTo[RecordAdded]
              })
              .map(r => InputMapped(namespace, metric, r.record))
          case UpdateSchemaFailed(_, _, errs) =>
            log.debug("Invalid schema for the metric {} and the record {}. Error are {}.",
                      metric,
                      record,
                      errs.mkString(","))
            Future(RecordRejected(namespace, metric, record, errs))
        }
        .pipeTo(sender())
    case msg @ DeleteNamespace(_) =>
      (namespaceDataActor ? msg)
        .mapTo[NamespaceDeleted]
        .flatMap(_ => namespaceSchemaActor ? msg)
        .mapTo[NamespaceDeleted]
        .pipeTo(sender())
    case msg @ ExecuteDeleteStatement(_) =>
      namespaceDataActor forward msg
    case msg @ DropMetric(_, _) =>
      namespaceDataActor forward (msg)
  }
}

trait JournalWriter

class AsyncJournalWriter {}
