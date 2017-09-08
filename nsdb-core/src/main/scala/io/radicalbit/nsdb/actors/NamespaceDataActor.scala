package io.radicalbit.nsdb.actors

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.NamespaceDataActor.commands._
import io.radicalbit.nsdb.actors.NamespaceDataActor.events.GetCount
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.coordinator.ReadCoordinator.{GetMetrics, GetNamespaces, NamespacesGot}
import io.radicalbit.nsdb.coordinator.WriteCoordinator.{
  DeleteNamespace,
  DropMetric,
  ExecuteDeleteStatement,
  NamespaceDeleted
}

import scala.concurrent.duration._
import scala.collection.mutable

class NamespaceDataActor(val basePath: String) extends Actor with ActorLogging {

  val indexerActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  private def getIndexer(namespace: String): ActorRef =
    indexerActors.getOrElse(
      namespace, {
        val indexerActor = context.actorOf(IndexerActor.props(basePath, namespace), s"indexer-service-$namespace")
        indexerActors += (namespace -> indexerActor)
        indexerActor
      }
    )

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.namespace-data.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  override def receive = {
    case GetNamespaces =>
      sender() ! NamespacesGot(indexerActors.keys.toSeq)
    case msg @ GetMetrics(namespace) =>
      getIndexer(namespace) forward msg
    case msg @ AddRecord(namespace, _, _) =>
      getIndexer(namespace).forward(msg)
    case msg @ AddRecords(namespace, _, _) =>
      getIndexer(namespace).forward(msg)
    case msg @ DeleteRecord(namespace, _, _) =>
      getIndexer(namespace).forward(msg)
    case msg @ DeleteMetric(namespace, _) =>
      getIndexer(namespace).forward(msg)
    case msg @ GetCount(namespace, _) =>
      getIndexer(namespace).forward(msg)
    case msg @ ExecuteDeleteStatement(statement) =>
      getIndexer(statement.namespace).forward(msg)
    case DeleteNamespace(namespace) =>
      val indexToRemove = getIndexer(namespace)
      (indexToRemove ? DeleteAllMetrics(namespace))
        .map(_ => {
          indexToRemove ! PoisonPill
          indexerActors -= namespace
          NamespaceDeleted(namespace)
        })
        .pipeTo(sender())
    case msg @ DropMetric(namespace, _) =>
      getIndexer(namespace).forward(msg)
    case msg @ ReadCoordinator.ExecuteSelectStatement(statement, _) =>
      getIndexer(statement.namespace).forward(msg)
  }
}

object NamespaceDataActor {
  def props(basePath: String): Props = Props(new NamespaceDataActor(basePath))

  object commands {
    case class AddRecord(namespace: String, metric: String, record: Bit)
    case class AddRecords(namespace: String, metric: String, records: Seq[Bit])
    case class DeleteRecord(namespace: String, metric: String, record: Bit)
    case class DeleteMetric(namespace: String, metric: String)
    case class DeleteAllMetrics(namespace: String)
  }

  object events {
    case class GetCount(namespace: String, metric: String)
    case class CountGot(namespace: String, metric: String, count: Int)
    case class RecordAdded(namespace: String, metric: String, record: Bit)
    case class RecordsAdded(namespace: String, metric: String, record: Seq[Bit])
    case class RecordRejected(namespace: String, metric: String, record: Bit, reasons: List[String])
    case class RecordDeleted(namespace: String, metric: String, record: Bit)
    case class MetricDeleted(namespace: String, metric: String)
    case class AllMetricsDeleted(namespace: String)
  }

}
