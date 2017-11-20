package io.radicalbit.nsdb.cluster.actor

import java.time.Duration
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

//  lazy val sharding: Boolean          = context.system.settings.config.getBoolean("nsdb.sharding.enabled")
//  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  val indexerActors: mutable.Map[(String, String), ActorRef] = mutable.Map.empty
//  val shards: mutable.Map[ShardKey, ActorRef]                = mutable.Map.empty

  private def getIndexer(db: String, namespace: String): ActorRef =
    indexerActors.getOrElse(
      (db, namespace), {
        val indexerActor =
          context.actorOf(IndexerActor.props(basePath, db, namespace), s"indexer-service-$db-$namespace")
        indexerActors += ((db, namespace) -> indexerActor)
        indexerActor
      }
    )

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.namespace-data.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  import context.dispatcher

//  override def receive: Receive = if (sharding) receiveShard else receiveNoShard
//
//  def receiveShard: Receive = Actor.emptyBehavior

  override def receive: Receive = {
    case GetNamespaces(db) =>
      sender() ! NamespacesGot(db, indexerActors.keys.filter(_._1 == db).map(_._2).toSeq)
    case msg @ GetMetrics(db, namespace) =>
      getIndexer(db, namespace) forward msg
    case msg @ AddRecord(db, namespace, _, _) =>
      getIndexer(db, namespace).forward(msg)
    case msg @ AddRecords(db, namespace, _, _) =>
      getIndexer(db, namespace).forward(msg)
    case msg @ DeleteRecord(db, namespace, _, _) =>
      getIndexer(db, namespace).forward(msg)
    case msg @ GetCount(db, namespace, _) =>
      getIndexer(db, namespace).forward(msg)
    case msg @ ExecuteDeleteStatementInternal(statement, _) =>
      getIndexer(statement.db, statement.namespace).forward(msg)
    case DeleteNamespace(db, namespace) =>
      val indexToRemove = getIndexer(db, namespace)
      (indexToRemove ? DeleteAllMetrics(db, namespace))
        .map(_ => {
          indexToRemove ! PoisonPill
          indexerActors -= ((db, namespace))
          NamespaceDeleted(db, namespace)
        })
        .pipeTo(sender())
    case msg @ DropMetric(db, namespace, _) =>
      getIndexer(db, namespace).forward(msg)
    case msg @ ExecuteSelectStatement(statement, _) =>
      getIndexer(statement.db, statement.namespace).forward(msg)
  }
}

object NamespaceDataActor {
  def props(basePath: String): Props = Props(new NamespaceDataActor(basePath))

}
