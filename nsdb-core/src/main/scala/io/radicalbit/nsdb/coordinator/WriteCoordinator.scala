package io.radicalbit.nsdb.coordinator

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.actors.NamespaceActor.{AddRecord, RecordAdded, RecordRejected}
import io.radicalbit.nsdb.actors.PublisherActor.RecordPublished
import io.radicalbit.nsdb.actors.SchemaCoordinatorActor.commands.UpdateSchemaFromRecord
import io.radicalbit.nsdb.actors.SchemaCoordinatorActor.events.{SchemaUpdated, UpdateSchemaFailed}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.coordinator.WriteCoordinator.InputMapped
import io.radicalbit.nsdb.model.Record

import scala.concurrent.Future

object WriteCoordinator {

  def props(schemaActor: ActorRef,
            commitLogService: ActorRef,
            namespaceActor: ActorRef,
            publisherActor: ActorRef): Props =
    Props(new WriteCoordinator(schemaActor, commitLogService, namespaceActor, publisherActor))

  sealed trait WriteCoordinatorProtocol

  case class FlatInput(ts: Long, namespace: String, metric: String, data: Array[Byte]) extends WriteCoordinatorProtocol

  case class MapInput(ts: Long, namespace: String, metric: String, record: Record)    extends WriteCoordinatorProtocol
  case class InputMapped(ts: Long, namespace: String, metric: String, record: Record) extends WriteCoordinatorProtocol
}

class WriteCoordinator(schemaActor: ActorRef,
                       commitLogService: ActorRef,
                       namespaceActor: ActorRef,
                       publisherActor: ActorRef)
    extends Actor
    with ActorLogging {

  import akka.pattern.ask

  import scala.concurrent.duration._

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  log.info("WriteCoordinator is ready.")

  override def receive = {
    case WriteCoordinator.MapInput(ts, namespace, metric, record) =>
      log.debug("Received a write request for (ts: {}, metric: {}, record : {})", ts, metric, record)
      (schemaActor ? UpdateSchemaFromRecord(namespace, metric, record))
        .flatMap {
          case SchemaUpdated(_, _) =>
            log.debug("Valid schema for the metric {} and the record {}", metric, record)
            (commitLogService ? CommitLogService.Insert(ts = ts, metric = metric, record = record))
              .mapTo[WroteToCommitLogAck]
              .flatMap(ack => {
                publisherActor ! RecordPublished(metric, record)
                (namespaceActor ? AddRecord(namespace, ack.metric, ack.record)).mapTo[RecordAdded]
              })
              .map(r =>
                InputMapped(r.record.timestamp, namespace, metric, record.copy(timestamp = r.record.timestamp)))
          case UpdateSchemaFailed(_, _, errs) =>
            log.debug("Invalid schema for the metric {} and the record {}. Error are {}.",
                      metric,
                      record,
                      errs.mkString(","))
            Future(RecordRejected(namespace, metric, record, errs))
        }
        .pipeTo(sender())
  }
}

trait JournalWriter

class AsyncJournalWriter {}
