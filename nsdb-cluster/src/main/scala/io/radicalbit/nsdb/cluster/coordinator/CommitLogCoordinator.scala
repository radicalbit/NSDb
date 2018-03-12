package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.commit_log.RollingCommitLogFileWriter

import scala.collection.mutable

object CommitLogCoordinator {

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

  def receive: Receive = {
    case WriteToCommitLog(db, namespace, metric, timestamp, InsertAction(bit)) =>
      getWriter(db, namespace).forward(InsertEntry(db, namespace, metric, timestamp, bit))
    case WriteToCommitLog(db, namespace, metric, timestamp, RejectAction(bit)) =>
      getWriter(db, namespace).forward(RejectEntry(db, namespace, metric, timestamp, bit))
    case WriteToCommitLog(db, namespace, metric, timestamp, DeleteAction(condition)) =>
      getWriter(db, namespace).forward(DeleteEntry(db, namespace, metric, timestamp, condition))
    case WriteToCommitLog(db, namespace, _, timestamp, DeleteNamespaceAction()) =>
      getWriter(db, namespace).forward(DeleteNamespaceEntry(db, namespace, timestamp))
    case WriteToCommitLog(db, namespace, metric, timestamp, DeleteMetricAction()) =>
      getWriter(db, namespace).forward(DeleteMetricEntry(db, namespace, metric, timestamp))
    case _ =>
      log.error("UnexpectedMessage")
  }
}
