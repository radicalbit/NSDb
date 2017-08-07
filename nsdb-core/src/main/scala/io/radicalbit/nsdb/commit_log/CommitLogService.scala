package io.radicalbit.nsdb.commit_log

import akka.actor.{Actor, ActorLogging, Props}
import akka.util.Timeout
import io.radicalbit.nsdb.commit_log.CommitLogEntry.Dimension
import io.radicalbit.nsdb.commit_log.CommitLogService.{Delete, Insert}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.util.Config._

object CommitLogService {

  def props = Props(new CommitLogService)

  sealed trait JournalServiceProtocol

  case class Insert(ts: Long, metric: String, dimensions: Map[String, java.io.Serializable])
      extends JournalServiceProtocol

  case class Delete(ts: Long, metric: String) extends JournalServiceProtocol

}

class CommitLogService() extends Actor with ActorLogging {

  import akka.pattern.{ask, pipe}
  import context.dispatcher

  import scala.concurrent.duration._

  implicit private val timeout = Timeout(1 second)
  implicit private val config  = context.system.settings.config

  private val commitLogWriterClass = getString(CommitLogWriterConf)

  private val commitLogWriterActor =
    context.system.actorOf(Props(Class.forName(commitLogWriterClass)), "commit-log-writer")

  def receive = {
    case msg @ Insert(ts, metric, dimensions) =>
      val entry = InsertNewEntry(ts = ts, metric = metric, dimensions = extractDimensions(msg))
      (commitLogWriterActor ? entry).mapTo[WroteToCommitLogAck].pipeTo(sender())

    // TODO: implement
    case Delete(ts, metric) => sys.error("Not Implemented")
  }

  private def extractDimensions(input: Insert): List[Dimension] =
    input.dimensions.map {
      case (k, v) => (k, v.getClass.getCanonicalName, v.toString.getBytes)
    }.toList
}
