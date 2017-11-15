package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.IndexerActor
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.collection.mutable

case class ShardKey(metric: String, interval: (Long, Long))

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

  override def receive: Receive = {
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
    case msg @ ExecuteSelectStatement(statement, _) =>
      getIndexer(statement.namespace).forward(msg)
  }
}

object NamespaceDataActor {
  def props(basePath: String): Props = Props(new NamespaceDataActor(basePath))

}
