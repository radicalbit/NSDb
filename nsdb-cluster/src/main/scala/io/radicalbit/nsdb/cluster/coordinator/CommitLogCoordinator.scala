package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.coordinator.CommitLogCoordinator.Insert
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WriteToCommitLogSucceeded
import io.radicalbit.nsdb.commit_log.InsertNewEntry
import io.radicalbit.nsdb.common.protocol.Bit

object CommitLogCoordinator {

  def props = Props[CommitLogCoordinator]

  sealed trait JournalServiceProtocol

  case class Insert(metric: String, bit: Bit) extends JournalServiceProtocol

}

class CommitLogCoordinator() extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  import akka.pattern.{ask, pipe}
  import context.dispatcher
  import io.radicalbit.nsdb.util.Config.{CommitLogWriterConf, getString}

  import scala.concurrent.duration._

  implicit private val timeout = Timeout(1 second)
  implicit private val config  = context.system.settings.config

  private val commitLogWriterClass = getString(CommitLogWriterConf)

  private val commitLogWriterActor =
    context.system.actorOf(Props(Class.forName(commitLogWriterClass)), "commit-log-coordinator")

  def receive = {
    case Insert(metric, bit) =>
      val entry = InsertNewEntry(metric = metric, bit = bit, replyTo = self)
      (commitLogWriterActor ? entry).mapTo[WriteToCommitLogSucceeded].pipeTo(sender())

  }
}
