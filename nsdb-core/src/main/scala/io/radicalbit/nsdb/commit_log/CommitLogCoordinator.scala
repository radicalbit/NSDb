package io.radicalbit.nsdb.commit_log

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.radicalbit.nsdb.commit_log.CommitLogCoordinator._
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{DeleteMetric, DeleteMetric => _, _}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.Condition

import scala.collection.mutable

object CommitLogCoordinator {

  sealed trait CommitLoggerAction
  case class InsertAction(bit : Bit) extends CommitLoggerAction
  case class RejectAction(bit: Bit) extends CommitLoggerAction
  case class DeleteAction(condition: Condition) extends CommitLoggerAction
  case object DeleteNamespaceAction extends CommitLoggerAction
  case object DeleteMetricAction extends CommitLoggerAction

  sealed trait JournalServiceProtocol

  sealed trait JournalServiceRequest extends JournalServiceProtocol
  sealed trait JournalServiceResponse extends JournalServiceProtocol

  case class WriteToCommitLog(db: String, namespace: String, metric: String, ts: Long,
    action: CommitLoggerAction) extends JournalServiceRequest

  case class WriteToCommitLogSucceeded(db: String, namespace: String, ts: Long, metric: String)
    extends JournalServiceResponse
  case class WriteToCommitLogFailed(db: String, namespace: String, ts: Long, metric: String, bit: Bit, reason: String)
    extends JournalServiceResponse

  def props(): Props = Props(new CommitLogCoordinator())
}

class CommitLogCoordinator extends Actor with ActorLogging {

  private val commitLoggerWriters: mutable.Map[String, ActorRef] = mutable.Map.empty

  private def getWriter(db: String, namespace: String): ActorRef = {
    commitLoggerWriters.getOrElse(
      s"$db-$namespace", {
        val cl = context.actorOf(RollingCommitLogFileWriter.props(db, namespace), s"commit-log-writer-$db-$namespace")
        commitLoggerWriters += (s"$db-$namespace" -> cl)
        cl
      }
    )
  }

  override def receive: Receive = {
    case msg @ WriteToCommitLog(db, namespace, metric, timestamp, action@InsertAction(bit)) =>
      getWriter(db, namespace).forward(InsertBit(db, namespace, metric, bit))
    case msg @ WriteToCommitLog(db, namespace, metric, timestamp, action@RejectAction(bit)) =>
      getWriter(db, namespace).forward(RejectBit(db, namespace, metric, bit ))
    case msg @ WriteToCommitLog(db, namespace, metric, timestamp, action@DeleteAction(condition)) =>
      getWriter(db, namespace).forward(DeleteBit(db, namespace, metric, timestamp, condition))
    case msg @ WriteToCommitLog(db, namespace, _, timestamp, action@DeleteNamespaceAction) =>
      getWriter(db, namespace).forward(DeleteNamespace(db, namespace, timestamp))
    case msg @ WriteToCommitLog(db, namespace, metric, timestamp, action@DeleteMetricAction) =>
      getWriter(db, namespace).forward(DeleteMetric(db, namespace, metric,  timestamp))
    case _ =>
      log.error("UnexpectedMessage")
  }
}
