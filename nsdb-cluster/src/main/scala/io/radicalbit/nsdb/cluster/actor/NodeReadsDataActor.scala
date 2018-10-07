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

import akka.actor.{ActorRef, Props}
import akka.pattern.{gracefulStop, pipe}
import akka.routing.{Broadcast, DefaultResizer, Pool, RoundRobinPool}
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.{MetricAccumulatorActor, MetricReaderActor}
import io.radicalbit.nsdb.cluster.actor.NodeReadsDataActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.concurrent.Future

/**
  * Actor responsible for dispatching read commands to the proper actor and index.
  * @param basePath indexes' root path.
  * @param nodeName String representation of the host and the port Actor is deployed at.
  */
class NodeReadsDataActor(val basePath: String, val nodeName: String) extends ActorPathLogging {

  lazy val readParallelism = ReadParallelism(context.system.settings.config.getConfig("nsdb.read.parallelism"))

  /**
    * Gets or creates reader child actor of class [[io.radicalbit.nsdb.actors.MetricReaderActor]] to handle read requests
    *
    * @param db database name
    * @param namespace namespace name
    * @return [[(ShardReaderActor, ShardAccumulatorActor)]] for selected database and namespace
    */
  private def getOrCreateReader(db: String, namespace: String): ActorRef = {
    val readerOpt = context.child(s"metric_reader_${db}_$namespace")

    readerOpt.getOrElse(
      context.actorOf(
        readParallelism.pool.props(
          MetricReaderActor
            .props(basePath, nodeName, db, namespace)
            .withDispatcher("akka.actor.control-aware-dispatcher")),
        s"metric_reader_${db}_$namespace"
      ))
  }

  /**
    * If exists, gets the reader for selected namespace and database.
    * Use in case of read
    *
    * @param db database name
    * @param namespace namespace name
    * @return Option containing child actor of class [[MetricAccumulatorActor]]
    */
  private def getReader(db: String, namespace: String): Option[ActorRef] =
    context.child(s"metric_reader_${db}_$namespace")

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
          getOrCreateReader(db, namespace)
      }
  }

  override def receive: Receive = {
    case GetDbs =>
      val dbs = context.children.collect { case c if c.path.name.split("_").length == 4 => c.path.name.split("_")(2) }
      sender() ! DbsGot(dbs.toSet)
    case GetNamespaces(db) =>
      val namespaces = context.children.collect {
        case a if a.path.name.startsWith("metric_reader") && a.path.name.split("_")(2) == db =>
          a.path.name.split("_")(3)
      }.toSet
      sender() ! NamespacesGot(db, namespaces)
    case msg @ GetMetrics(db, namespace) =>
      getReader(db, namespace) match {
        case Some(child) => child forward msg
        case None        => sender() ! MetricsGot(db, namespace, Set.empty)
      }
    case DeleteNamespace(db, namespace) =>
      val child = getReader(db, namespace)
      child
        .map(gracefulStop(_, timeout.duration))
        .getOrElse(Future(true))
        .map(_ => NamespaceDeleted(db, namespace))
        .pipeTo(sender())
    case DropMetric(db, namespace, metric) =>
      val child = getReader(db, namespace)
      child
        .map(gracefulStop(_, timeout.duration))
        .getOrElse(Future(true))
        .map(_ => MetricDropped(db, namespace, metric))
        .pipeTo(sender())
    case msg @ GetCount(db, namespace, metric) =>
      getReader(db, namespace) match {
        case Some(child) => child forward msg
        case None        => sender() ! CountGot(db, namespace, metric, 0)
      }
    case msg @ ExecuteSelectStatement(statement, _, _) =>
      getReader(statement.db, statement.namespace) match {
        case Some(child) => child forward msg
        case None        => sender() ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, Seq.empty)
      }

    case msg @ Refresh(_, locations) =>
      locations.groupBy(l => (l.coordinates.db, l.coordinates.namespace)).foreach {
        case ((db, namespace), _) =>
          getOrCreateReader(db, namespace) ! Broadcast(msg)
      }
  }

}

object NodeReadsDataActor {

  /**
    * Case class to model reader router size.
    * @param initialSize routees initial size.
    * @param lowerBound min number of routees.
    * @param upperBound max number of routees.
    */
  case class ReadParallelism(initialSize: Int, lowerBound: Int, upperBound: Int) {

    /**
      * @return a [[Pool]] from size members.
      */
    def pool: Pool = RoundRobinPool(initialSize, Some(DefaultResizer(lowerBound, upperBound)))
  }

  object ReadParallelism {
    def apply(enclosingConfig: Config): ReadParallelism =
      ReadParallelism(enclosingConfig.getInt("initial-size"),
                      enclosingConfig.getInt("lower-bound"),
                      enclosingConfig.getInt("upper-bound"))
  }

  def props(basePath: String, nodeName: String): Props =
    Props(new NodeReadsDataActor(basePath, nodeName))

  case class AddRecordToLocation(db: String, namespace: String, bit: Bit, location: Location)
  case class DeleteRecordFromLocation(db: String, namespace: String, bit: Bit, location: Location)
  case class ExecuteDeleteStatementInternalInLocations(statement: DeleteSQLStatement,
                                                       schema: Schema,
                                                       locations: Seq[Location])
}