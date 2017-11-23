package io.radicalbit.nsdb.cluster.actor

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.IndexerActor
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor.{AddRecordToLocation, DeleteRecordFromLocation}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.collection.mutable

case class NamespaceKey(db: String, namespace: String)

class NamespaceDataActor(val basePath: String) extends Actor with ActorLogging {

  lazy val sharding: Boolean = context.system.settings.config.getBoolean("nsdb.sharding.enabled")

  val childActors: mutable.Map[NamespaceKey, ActorRef] = mutable.Map.empty

  private def getChild(db: String, namespace: String): ActorRef =
    childActors.getOrElse(
      NamespaceKey(db, namespace), {
        val child =
          if (sharding) context.actorOf(ShardActor.props(basePath, db, namespace), s"shard-service-$db-$namespace")
          else
            context.actorOf(IndexerActor.props(basePath, db, namespace), s"indexer-service-$db-$namespace")
        childActors += (NamespaceKey(db, namespace) -> child)
        child
      }
    )

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.namespace-data.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  import context.dispatcher

  override def preStart() = {
    val childs = Paths.get(basePath).toFile.list().flatMap(db => {
      Paths.get(basePath, db).toFile.list().map(namespace => (db,namespace))
    })
    childs.foreach { case (db, namespace) =>
      childActors +=  (NamespaceKey(db,namespace) ->  (if (sharding) context.actorOf(ShardActor.props(basePath, db, namespace), s"shard-service-$db-$namespace")
    else
      context.actorOf(IndexerActor.props(basePath, db, namespace), s"indexer-service-$db-$namespace")))
    }
  }

  override def receive: Receive = commons orElse (if (sharding) receiveShard else receiveNoShard)

  def commons: Receive = {
    case GetNamespaces(db) =>
      sender() ! NamespacesGot(db, childActors.keys.filter(_.db == db).map(_.namespace).toSet)
    case msg @ GetMetrics(db, namespace) =>
      getChild(db, namespace) forward msg
    case msg @ ExecuteDeleteStatement(statement) =>
      getChild(statement.db, statement.namespace).forward(msg)
    case DeleteNamespace(db, namespace) =>
      val indexToRemove = getChild(db, namespace)
      (indexToRemove ? DeleteAllMetrics(db, namespace))
        .map(_ => {
          indexToRemove ! PoisonPill
          childActors -= NamespaceKey(db, namespace)
          NamespaceDeleted(db, namespace)
        })
        .pipeTo(sender())
    case msg @ DropMetric(db, namespace, _) =>
      getChild(db, namespace).forward(msg)
    case msg @ GetCount(db, namespace, _) =>
      getChild(db, namespace).forward(msg)
    case msg @ ExecuteSelectStatement(statement, _) =>
      getChild(statement.db, statement.namespace).forward(msg)
  }

  def receiveShard: Receive = {
    case msg @ AddRecordToLocation(db, namespace, _, _, _) =>
      getChild(db, namespace).forward(msg)
    case msg @ DeleteRecordFromLocation(db, namespace, _, _, _) =>
      getChild(db, namespace).forward(msg)
  }

  def receiveNoShard: Receive = {
    case msg @ AddRecord(db, namespace, _, _) =>
      getChild(db, namespace).forward(msg)
    case msg @ AddRecords(db, namespace, _, _) =>
      getChild(db, namespace).forward(msg)
    case msg @ DeleteRecord(db, namespace, _, _) =>
      getChild(db, namespace).forward(msg)
    case msg @ GetCount(db, namespace, _) =>
      getChild(db, namespace).forward(msg)
    case msg @ ExecuteSelectStatement(statement, _) =>
      getChild(statement.db, statement.namespace).forward(msg)
  }
}

object NamespaceDataActor {
  def props(basePath: String): Props = Props(new NamespaceDataActor(basePath))

  case class AddRecordToLocation(db: String, namespace: String, metric: String, bit: Bit, location: Location)
  case class DeleteRecordFromLocation(db: String, namespace: String, metric: String, bit: Bit, location: Location)
}
