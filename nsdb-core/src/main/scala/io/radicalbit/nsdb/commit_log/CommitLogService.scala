package io.radicalbit.commit_log

import akka.actor.{Actor, ActorLogging, Props}
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService.{Delete, Insert}
import io.radicalbit.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.model.Record
import io.radicalbit.util.Config._

object CommitLogService {

  def props = Props(new CommitLogService)

  sealed trait JournalServiceProtocol

  case class Insert(ts: Long, metric: String, record: Record) extends JournalServiceProtocol

  case class Delete(ts: Long, metric: String) extends JournalServiceProtocol

}

class CommitLogService() extends Actor with ActorLogging {

  import akka.pattern.{ask, pipe}
  import scala.concurrent.duration._
  import context.dispatcher

  implicit private val timeout = Timeout(1 second)
  implicit private val config  = context.system.settings.config

  private val commitLogWriterClass = getString(CommitLogWriterConf)

  private val commitLogWriterActor =
    context.system.actorOf(Props(Class.forName(commitLogWriterClass)), "commit-log-writer")

  def receive = {
    case msg @ Insert(ts, metric, record) =>
      val entry = InsertNewEntry(ts = ts, metric = metric, record = record)
      (commitLogWriterActor ? entry).mapTo[WroteToCommitLogAck].pipeTo(sender())

    // TODO: implement
    case Delete(ts, metric) => sys.error("Not Implemented")
  }
}
