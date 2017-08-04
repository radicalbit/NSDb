package io.radicalbit.coordinator

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.commit_log.CommitLogWriterActor.WroteToCommitLogAck

object WriteCoordinator {

  def props(commitLogService: ActorRef): Props = Props(new WriteCoordinator(commitLogService))

  sealed trait WriteCoordinatorProtocol

  case class FlatInput(ts: Long, metric: String, data: Array[Byte]) extends WriteCoordinatorProtocol

  case class MapInput(ts: Long, metric: String, dimensions: Map[String, java.io.Serializable])
      extends WriteCoordinatorProtocol

}

class WriteCoordinator(commitLogService: ActorRef) extends Actor with ActorLogging {

  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  log.info("WriteCoordinator is ready.")

  override def receive = {
    case msg @ WriteCoordinator.MapInput(ts, metric, dimensions) =>
      log.debug("Received a write request for (ts: {}, metric: {})", ts, metric)

      (commitLogService ? CommitLogService.Insert(ts = ts, metric = metric, dimensions = dimensions))
        .mapTo[WroteToCommitLogAck] foreach { ack =>
        log.debug("Received the following ack {}", ack)
      }
  }
}

trait JournalWriter

class AsyncJournalWriter {}
