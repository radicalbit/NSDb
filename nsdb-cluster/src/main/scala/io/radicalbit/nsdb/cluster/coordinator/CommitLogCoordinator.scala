package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.cluster.coordinator.CommitLogCoordinator.{
  Insert,
  SubscribeWriter,
  WriteToCommitLogSucceeded,
  WriterSubscribed
}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{InsertNewEntry, NewEntryInserted}
import io.radicalbit.nsdb.commit_log.InsertEntry
import io.radicalbit.nsdb.common.protocol.Bit

import scala.collection.mutable

object CommitLogCoordinator {

  def props: Props = Props[CommitLogCoordinator]

  sealed trait JournalServiceProtocol

  case class SubscribeWriter(nameNode: String, actor: ActorRef)
  case class WriterSubscribed(nameNode: String, actor: ActorRef)

  case class Insert(metric: String, bit: Bit) extends JournalServiceProtocol
  case class Commit(metric: String, bit: Bit) extends JournalServiceProtocol
  case class Reject(metric: String, bit: Bit) extends JournalServiceProtocol

  sealed trait JournalServiceResponse extends JournalServiceProtocol

  case class WriteToCommitLogSucceeded(ts: Long, metric: String, bit: Bit)              extends JournalServiceResponse
  case class WriteToCommitLogFailed(ts: Long, metric: String, bit: Bit, reason: String) extends JournalServiceResponse

}

class CommitLogCoordinator() extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  import scala.concurrent.duration._

  implicit private val timeout: Timeout = Timeout(10.second)
  implicit private val config: Config   = context.system.settings.config

  private var writers: mutable.Map[String, ActorRef] = mutable.Map.empty

  private var acks: Int = 0

  def receive: Receive = write

  def subscribe: Receive = {
    case SubscribeWriter(nameNode, actor) =>
      writers += (nameNode -> actor)
      sender ! WriterSubscribed(nameNode, actor)
  }

  def write: Receive = {
    case Insert(metric, bit) =>
      context.become(waitForAck)
      writers.foreach {
        case (_, actor) =>
          actor ! InsertNewEntry(InsertEntry(metric = metric, bit = bit), replyTo = self)
      }
  }

  def waitForAck: Receive = {
    case NewEntryInserted(InsertEntry(metric, bit), replyTo) =>
      acks += 1
      if (acks == writers.size) {
        acks = 0
        replyTo ! WriteToCommitLogSucceeded(bit.timestamp, metric, bit)
        context.become(write)
      }
  }
}
