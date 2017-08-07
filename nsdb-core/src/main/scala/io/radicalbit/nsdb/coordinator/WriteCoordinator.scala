package io.radicalbit.coordinator

import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.index.IndexerActor.{RecordAdded, RecordRejected}
import io.radicalbit.index.{Schema, SchemaIndex}
import io.radicalbit.model.Record
import org.apache.lucene.store.FSDirectory

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

  log.info("WriteCoordinator is ready.")

  override def receive = {
    case msg @ WriteCoordinator.MapInput(ts, metric, record) =>
      log.debug("Received a write request for (ts: {}, metric: {})", ts, metric)

      println(msg.record)

      val schemaValidation = Schema(metric, record) flatMap { newSchema =>
        if (schemaIndex.getSchema(metric).forall(schemaIndex.isValidSchema(_, newSchema))) Success(newSchema)
        else
          Failure(new RuntimeException(s"schema $newSchema is incompatible"))
      }
      println(schemaValidation)
      schemaValidation match {
        case Success(newSchema) =>
          implicit val writer = schemaIndex.getWriter
          schemaIndex.update(metric, newSchema)
          writer.close
          (commitLogService ? CommitLogService.Insert(ts = ts, metric = metric, record = record))
            .mapTo[WroteToCommitLogAck]
            .map(ack => RecordAdded(ack.metric, ack.record))
            .pipeTo(sender())
        case Failure(ex) => sender() ! RecordRejected(metric, record, ex.getMessage)
      }
    case WriteCoordinator.GetSchema(metric) =>
      sender ! WriteCoordinator.SchemaGot(metric, schemaIndex.getSchema(metric))
  }
}

trait JournalWriter

class AsyncJournalWriter {}
