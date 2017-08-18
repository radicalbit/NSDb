package io.radicalbit.nsdb.actors

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.NamespaceActor._
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.model.Record

import scala.concurrent.duration._
import scala.collection.mutable

class NamespaceActor(val basePath: String) extends Actor with ActorLogging {

  val indexerActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  private def getIndexer(namespace: String): ActorRef =
    indexerActors.getOrElse(
      namespace, {
        val indexerActor = context.actorOf(IndexerActor.props(basePath, namespace), s"indexer-service-$namespace")
        indexerActors += (namespace -> indexerActor)
        indexerActor
      }
    )

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  override def receive = {
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
    case DeleteNamespace(namespace) =>
      val indexToRemove = getIndexer(namespace)
      (indexToRemove ? DeleteAllMetrics(namespace))
        .map(_ => {
          indexToRemove ! PoisonPill
          indexerActors -= namespace
          NamespaceDeleted(namespace)
        })
        .pipeTo(sender())
    case msg @ ReadCoordinator.ExecuteSelectStatement(namespace, _, _) =>
      getIndexer(namespace).forward(msg)
  }
}

object NamespaceActor {
  def props(basePath: String): Props = Props(new NamespaceActor(basePath))

  case class AddRecord(namespace: String, metric: String, record: Record)
  case class AddRecords(namespace: String, metric: String, records: Seq[Record])
  case class DeleteRecord(namespace: String, metric: String, record: Record)
  case class DeleteMetric(namespace: String, metric: String)
  case class DeleteAllMetrics(namespace: String)
  case class DeleteNamespace(namespace: String)
  case class GetCount(namespace: String, metric: String)
  case class CountGot(namespace: String, metric: String, count: Int)
  case class RecordAdded(namespace: String, metric: String, record: Record)
  case class RecordsAdded(namespace: String, metric: String, record: Seq[Record])
  case class RecordRejected(namespace: String, metric: String, record: Record, reasons: List[String])
  case class RecordDeleted(namespace: String, metric: String, record: Record)
  case class MetricDeleted(namespace: String, metric: String)
  case class AllMetricsDeleted(namespace: String)
  case class NamespaceDeleted(namespace: String)
}
