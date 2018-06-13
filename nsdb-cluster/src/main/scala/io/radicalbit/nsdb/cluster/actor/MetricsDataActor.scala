/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.cluster.actor

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.{ShardAccumulatorActor, ShardKey, ShardReaderActor}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor._
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.concurrent.Future

/**
  * Actor responsible for dispatching read or write commands to the proper actor and index.
  * @param basePath indexes' root path.
  */
class MetricsDataActor(val basePath: String) extends Actor with ActorLogging {

  lazy val sharding: Boolean = context.system.settings.config.getBoolean("nsdb.sharding.enabled")

  /**
    * Gets or creates reader child actor of class [[io.radicalbit.nsdb.actors.ShardReaderActor]] to handle read requests
    *
    * @param db database name
    * @param namespace namespace name
    * @return [[(ShardReaderActor, ShardAccumulatorActor)]] for selected database and namespace
    */
  private def getOrCreateChildren(db: String, namespace: String): (ActorRef, ActorRef) = {
    val readerOpt      = context.child(s"shard_reader_${db}_$namespace")
    val accumulatorOpt = context.child(s"shard_accumulator_${db}_$namespace")

    val reader = readerOpt.getOrElse(
      context.actorOf(ShardReaderActor.props(basePath, db, namespace), s"shard_reader_${db}_$namespace"))
    val accumulator = accumulatorOpt.getOrElse(
      context.actorOf(ShardAccumulatorActor.props(basePath, db, namespace, reader),
                      s"shard_accumulator_${db}_$namespace"))
    (reader, accumulator)
  }

  private def getChildren(db: String, namespace: String): (Option[ActorRef], Option[ActorRef]) =
    (context.child(s"shard_reader_${db}_$namespace"), context.child(s"shard_accumulator_${db}_$namespace"))

  /**
    * If exists, gets the reader for selected namespace and database.
    * Use in case of read
    *
    * @param db database name
    * @param namespace namespace name
    * @return Option containing child actor of class [[ShardAccumulatorActor]]
    */
  private def getReader(db: String, namespace: String): Option[ActorRef] =
    context.child(s"shard_reader_${db}_$namespace")

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.namespace-data.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  import context.dispatcher

  override def preStart(): Unit = {
    Option(Paths.get(basePath).toFile.list())
      .map(_.toSet)
      .getOrElse(Set.empty)
      .filter(f => Paths.get(basePath, f).toFile.isDirectory)
      .flatMap(db => {
        Paths.get(basePath, db).toFile.list().map(namespace => (db, namespace))
      })
      .foreach {
        case (db, namespace) =>
          getOrCreateChildren(db, namespace)
      }
  }

  override def receive: Receive = commons orElse shardBehaviour

  def commons: Receive = {
    case GetDbs =>
      val dbs = context.children.collect { case c if c.path.name.split("_").length == 4 => c.path.name.split("_")(2) }
      sender() ! DbsGot(dbs.toSet)
    case GetNamespaces(db) =>
      val namespaces = context.children.collect {
        case a if a.path.name.startsWith("shard_reader") && a.path.name.split("_")(2) == db =>
          a.path.name.split("_")(3)
      }.toSet
      sender() ! NamespacesGot(db, namespaces)
    case msg @ GetMetrics(db, namespace) =>
      getReader(db, namespace) match {
        case Some(child) => child forward msg
        case None        => sender() ! MetricsGot(db, namespace, Set.empty)
      }
    case DeleteNamespace(db, namespace) =>
      val children = getChildren(db, namespace)
      val f = children._2
        .map(indexToRemove => indexToRemove ? DeleteAllMetrics(db, namespace))
        .getOrElse(Future(AllMetricsDeleted(db, namespace)))
      f.map(_ => {
          children._1.foreach(_ ! PoisonPill)
          children._2.foreach(_ ! PoisonPill)
          NamespaceDeleted(db, namespace)
        })
        .pipeTo(sender())
    case msg @ DropMetric(db, namespace, _) =>
      getOrCreateChildren(db, namespace)._2 forward msg
    case msg @ GetCount(db, namespace, metric) =>
      getReader(db, namespace) match {
        case Some(child) => child forward msg
        case None        => sender() ! CountGot(db, namespace, metric, 0)
      }
    case msg @ ExecuteSelectStatement(statement, _) =>
      getReader(statement.db, statement.namespace) match {
        case Some(child) => child forward msg
        case None        => sender() ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, Seq.empty)
      }
  }

  def shardBehaviour: Receive = {
    case AddRecordToLocation(db, namespace, bit, location) =>
      getOrCreateChildren(db, namespace)._2
        .forward(AddRecordToShard(db, namespace, ShardKey(location.metric, location.from, location.to), bit))
    case DeleteRecordFromLocation(db, namespace, bit, location) =>
      getOrCreateChildren(db, namespace)._2
        .forward(DeleteRecordFromShard(db, namespace, ShardKey(location.metric, location.from, location.to), bit))
    case ExecuteDeleteStatementInternalInLocations(statement, schema, locations) =>
      getOrCreateChildren(statement.db, statement.namespace)._2.forward(
        ExecuteDeleteStatementInShards(statement, schema, locations.map(l => ShardKey(l.metric, l.from, l.to))))
  }

}

object MetricsDataActor {
  def props(basePath: String): Props = Props(new MetricsDataActor(basePath))

  case class AddRecordToLocation(db: String, namespace: String, bit: Bit, location: Location)
  case class DeleteRecordFromLocation(db: String, namespace: String, bit: Bit, location: Location)
  case class ExecuteDeleteStatementInternalInLocations(statement: DeleteSQLStatement,
                                                       schema: Schema,
                                                       locations: Seq[Location])
}
