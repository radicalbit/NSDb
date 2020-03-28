/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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
import akka.pattern.{ask, gracefulStop, pipe}
import akka.routing.{DefaultResizer, Pool, RoundRobinPool}
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.actors.{MetricAccumulatorActor, MetricReaderActor}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor._
import io.radicalbit.nsdb.util.{FileUtils => NSDbFileUtils}
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.ActorPathLogging
import org.apache.commons.io.FileUtils

import scala.concurrent.{ExecutionContext, Future}

/**
  * Actor responsible for dispatching read or write commands to the proper actor and index.
  * @param basePath indexes' root path.
  * @param commitLogCoordinator Commit log coordinator reference.
  */
class MetricsDataActor(val basePath: String, val nodeName: String, commitLogCoordinator: ActorRef)
    extends ActorPathLogging {

  lazy val readParallelism = ReadParallelism(context.system.settings.config.getConfig("nsdb.read.parallelism"))

  /**
    * Gets or creates reader child actor of class [[MetricReaderActor]] to handle read requests
    *
    * @param db database name
    * @param namespace namespace name
    * @return [[MetricReaderActor]] for selected database and namespace
    */
  private def getOrCreateReader(db: String, namespace: String): ActorRef =
    context
      .child(s"metric_reader_${db}_$namespace")
      .getOrElse(
        context.actorOf(
          readParallelism.pool.props(
            MetricReaderActor
              .props(basePath, nodeName, db, namespace)
              .withDispatcher("akka.actor.control-aware-dispatcher")),
          s"metric_reader_${db}_$namespace"
        ))

  /**
    * Gets or creates accumulator child actor of class [[MetricAccumulatorActor]] to handle write requests
    *
    * @param db database name
    * @param namespace namespace name
    * @return [[MetricAccumulatorActor]] for selected database and namespace
    */
  private def getOrCreateAccumulator(db: String, namespace: String): ActorRef =
    context.child(s"metric_accumulator_${db}_$namespace").getOrElse {
      val reader = context
        .child(s"metric_reader_${db}_$namespace")
        .getOrElse(context.actorOf(
          readParallelism.pool.props(MetricReaderActor
            .props(basePath, nodeName, db, namespace)
            .withDispatcher("akka.actor.control-aware-dispatcher")),
          s"metric_reader_${db}_$namespace"
        ))
      context.actorOf(MetricAccumulatorActor.props(basePath, db, namespace, reader, commitLogCoordinator),
                      s"metric_accumulator_${db}_$namespace")
    }

  /**
    * Gracefully stops reader and accumulator children.
    * @param db database name
    * @param namespace namespace name
    */
  private def stopChildren(db: String, namespace: String)(implicit ec: ExecutionContext) = {
    Future.sequence(
      Seq(
        context.child(s"metric_reader_${db}_$namespace").map(gracefulStop(_, timeout.duration)).getOrElse(Future(true)),
        context
          .child(s"metric_accumulator_${db}_$namespace")
          .map(gracefulStop(_, timeout.duration))
          .getOrElse(Future(true))
      ))
  }

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.namespace-data.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  import context.dispatcher

  override def receive: Receive = {
    case DeleteNamespace(db, namespace) =>
      (getOrCreateAccumulator(db, namespace) ? DeleteAllMetrics(db, namespace))
        .flatMap(_ => stopChildren(db, namespace))
        .map(_ => NamespaceDeleted(db, namespace))
        .pipeTo(sender())
    case msg @ DropMetricWithLocations(db, namespace, _, _) =>
      getOrCreateAccumulator(db, namespace) forward msg
    case msg @ EvictShard(db, namespace, _) =>
      getOrCreateAccumulator(db, namespace) forward msg
    case msg @ GetCountWithLocations(db, namespace, _, _) =>
      getOrCreateReader(db, namespace) forward msg
    case msg @ ExecuteSelectStatement(statement, _, _, _) =>
      log.debug("executing statement in metric data actor {}", statement)
      getOrCreateReader(statement.db, statement.namespace) forward msg
    case AddRecordToLocation(db, namespace, bit, location) =>
      getOrCreateAccumulator(db, namespace) forward AddRecordToShard(
        db,
        namespace,
        Location(location.metric, nodeName, location.from, location.to),
        bit)
    case DeleteRecordFromLocation(db, namespace, bit, location) =>
      getOrCreateAccumulator(db, namespace) forward DeleteRecordFromShard(
        db,
        namespace,
        Location(location.metric, nodeName, location.from, location.to),
        bit)
    case ExecuteDeleteStatementInternalInLocations(statement, schema, locations) =>
      getOrCreateAccumulator(statement.db, statement.namespace) forward ExecuteDeleteStatementInShards(
        statement,
        schema,
        locations.map(l => Location(l.metric, nodeName, l.from, l.to)))
  }

}

object MetricsDataActor {

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

  def props(basePath: String, nodeName: String, commitLogCoordinator: ActorRef): Props =
    Props(new MetricsDataActor(basePath, nodeName, commitLogCoordinator))

  case class AddRecordToLocation(db: String, namespace: String, bit: Bit, location: Location) extends NSDbSerializable
  case class DeleteRecordFromLocation(db: String, namespace: String, bit: Bit, location: Location)
      extends NSDbSerializable
  case class ExecuteDeleteStatementInternalInLocations(statement: DeleteSQLStatement,
                                                       schema: Schema,
                                                       locations: Seq[Location])
      extends NSDbSerializable
}
