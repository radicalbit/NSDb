package io.radicalbit.coordinator

import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.index.IndexerActor.{AddRecord, RecordAdded, RecordRejected}
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}
import io.radicalbit.nsdb.model.Record
import org.apache.lucene.store.FSDirectory

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

object WriteCoordinator {

  def props(basePath: String, commitLogService: ActorRef, indexerActor: ActorRef): Props =
    Props(new WriteCoordinator(basePath, commitLogService, indexerActor))

  sealed trait WriteCoordinatorProtocol

  case class FlatInput(ts: Long, metric: String, data: Array[Byte]) extends WriteCoordinatorProtocol

  case class MapInput(ts: Long, metric: String, record: Record) extends WriteCoordinatorProtocol

  case class GetSchema(metric: String)                         extends WriteCoordinatorProtocol
  case class SchemaGot(metric: String, schema: Option[Schema]) extends WriteCoordinatorProtocol

}

class WriteCoordinator(basePath: String, commitLogService: ActorRef, indexerActor: ActorRef)
    extends Actor
    with ActorLogging {

  import akka.pattern.ask

  import scala.concurrent.duration._

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  private lazy val schemaIndex = new SchemaIndex(FSDirectory.open(Paths.get(basePath, "schemas")))

  private lazy val schemas: mutable.Map[String, Schema] = mutable.Map.empty

  log.info("WriteCoordinator is ready.")

  override def preStart(): Unit = {
    super.preStart()
    schemas ++ schemaIndex.getAllSchemas.map(s => s.metric -> s).toMap
  }

  private def getSchema(metric: String) = schemas.get(metric) orElse schemaIndex.getSchema(metric)

  override def receive = {
    case WriteCoordinator.MapInput(ts, metric, record) =>
      log.debug("Received a write request for (ts: {}, metric: {})", ts, metric)

      val schemaValidation = Schema(metric, record) flatMap { newSchema =>
        if (getSchema(metric).forall(schemaIndex.isValidSchema(_, newSchema))) Success(newSchema)
        else
          Failure(new RuntimeException(s"schema $newSchema is incompatible"))
      }
      schemaValidation match {
        case Success(newSchema) =>
          implicit val writer = schemaIndex.getWriter
          schemas += (newSchema.metric -> newSchema)
          Future { schemaIndex.update(metric, newSchema) }
          writer.close
          (commitLogService ? CommitLogService.Insert(ts = ts, metric = metric, record = record))
            .mapTo[WroteToCommitLogAck]
            .flatMap(ack => (indexerActor ? AddRecord(ack.metric, ack.record)).mapTo[RecordAdded])
            .pipeTo(sender())
        case Failure(ex) => sender() ! RecordRejected(metric, record, ex.getMessage)
      }
    case WriteCoordinator.GetSchema(metric) =>
      sender ! WriteCoordinator.SchemaGot(metric, getSchema(metric))
  }
}

trait JournalWriter

class AsyncJournalWriter {}
