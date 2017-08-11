package io.radicalbit.nsdb.coordinator

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.util.Timeout
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.actors.IndexerActor.{AddRecord, RecordAdded, RecordRejected}
import io.radicalbit.nsdb.actors.SchemaSupport
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.index.Schema
import io.radicalbit.nsdb.model.Record

import scala.concurrent.Future

object WriteCoordinator {

  def props(basePath: String, commitLogService: ActorRef, indexerActor: ActorRef): Props =
    Props(new WriteCoordinator(basePath, commitLogService, indexerActor))

  sealed trait WriteCoordinatorProtocol

  case class FlatInput(ts: Long, metric: String, data: Array[Byte]) extends WriteCoordinatorProtocol

  case class MapInput(ts: Long, metric: String, record: Record) extends WriteCoordinatorProtocol

  case class GetSchema(metric: String)                         extends WriteCoordinatorProtocol
  case class SchemaGot(metric: String, schema: Option[Schema]) extends WriteCoordinatorProtocol

}

class WriteCoordinator(val basePath: String, commitLogService: ActorRef, indexerActor: ActorRef)
    extends Actor
    with ActorLogging
    with SchemaSupport {

  import akka.pattern.ask

  import scala.concurrent.duration._

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  log.info("WriteCoordinator is ready.")

  private def getSchema(metric: String) = schemas.get(metric) orElse schemaIndex.getSchema(metric)

  override def receive = {
    case WriteCoordinator.MapInput(ts, metric, record) =>
      log.debug("Received a write request for (ts: {}, metric: {})", ts, metric)
      (Schema(metric, record), getSchema(metric)) match {
        case (Valid(newSchema), Some(oldSchema)) =>
          schemaIndex.isCompatibleSchema(oldSchema, newSchema) match {
            case Valid(_) =>
              implicit val writer = schemaIndex.getWriter
              schemas += (newSchema.metric -> newSchema)
              Future { schemaIndex.update(metric, newSchema) }
              writer.close()
              (commitLogService ? CommitLogService.Insert(ts = ts, metric = metric, record = record))
                .mapTo[WroteToCommitLogAck]
                .flatMap(ack => (indexerActor ? AddRecord(ack.metric, ack.record)).mapTo[RecordAdded])
                .pipeTo(sender())
            case Invalid(errs) => sender() ! RecordRejected(metric, record, errs.toList)
          }
        case (Valid(newSchema), None) =>
          implicit val writer = schemaIndex.getWriter
          schemas += (newSchema.metric -> newSchema)
          Future { schemaIndex.update(metric, newSchema) }
          writer.close()
          (commitLogService ? CommitLogService.Insert(ts = ts, metric = metric, record = record))
            .mapTo[WroteToCommitLogAck]
            .flatMap(ack => (indexerActor ? AddRecord(ack.metric, ack.record)).mapTo[RecordAdded])
            .pipeTo(sender())
        case (Invalid(errs), _) => sender() ! RecordRejected(metric, record, errs.toList)
      }
    case WriteCoordinator.GetSchema(metric) =>
      sender ! WriteCoordinator.SchemaGot(metric, getSchema(metric))
  }
}

trait JournalWriter

class AsyncJournalWriter {}
