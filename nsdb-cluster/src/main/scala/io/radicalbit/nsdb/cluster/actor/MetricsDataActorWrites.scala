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
import akka.pattern.{ask, gracefulStop, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.MetricAccumulatorActor
import io.radicalbit.nsdb.cluster.actor.MetricsDataActorReads._
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.concurrent.Future

/**
  * Actor responsible for dispatching read or write commands to the proper actor and index.
  * @param basePath indexes' root path.
  * @param nodeName String representation of the host and the port Actor is deployed at.
  */
class MetricsDataActorWrites(val basePath: String,
                             val nodeName: String,
                             localCommitLogCoordinator: ActorRef,
                             metricsReaderActor: ActorRef)
    extends ActorPathLogging {

  /**
    * Gets or creates reader child actor of class [[io.radicalbit.nsdb.actors.MetricReaderActor]] to handle read requests
    *
    * @param db database name
    * @param namespace namespace name
    * @return [[(ShardReaderActor, ShardAccumulatorActor)]] for selected database and namespace
    */
  private def getOrCreateAccumulator(db: String, namespace: String): ActorRef = {
    val accumulatorOpt = context.child(s"metric_accumulator_${db}_$namespace")

    accumulatorOpt.getOrElse(
      context.actorOf(
        MetricAccumulatorActor.props(basePath, db, namespace, metricsReaderActor, localCommitLogCoordinator),
        s"metric_accumulator_${db}_$namespace"))
  }

  private def getAccumulator(db: String, namespace: String): Option[ActorRef] =
    context.child(s"metric_accumulator_${db}_$namespace")

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
          getOrCreateAccumulator(db, namespace)
      }
  }

  override def receive: Receive = {
    case msg @ DeleteNamespace(db, namespace) =>
      val accumulator = getAccumulator(db, namespace)
      if (accumulator.isEmpty)
        sender() ! Future(AllMetricsDeleted(db, namespace))
      else
        (accumulator.get ? DeleteAllMetrics(db, namespace))
          .flatMap(_ => {
            Future.sequence(Seq(metricsReaderActor ? msg, gracefulStop(accumulator.get, timeout.duration)))
          })
          .map(_ => NamespaceDeleted(db, namespace))
          .pipeTo(sender())
    case msg @ DropMetric(db, namespace, _) =>
      (getOrCreateAccumulator(db, namespace) ? msg)
        .map { res =>
          metricsReaderActor ! msg
          res
        }
        .pipeTo(sender())
    case msg @ AddRecordToLocation(db, namespace, bit, location) =>
      log.debug("received message {}", msg)
      getOrCreateAccumulator(db, namespace)
        .forward(
          AddRecordToShard(db, namespace, Location(location.coordinates, nodeName, location.from, location.to), bit))
    case DeleteRecordFromLocation(db, namespace, bit, location) =>
      getOrCreateAccumulator(db, namespace)
        .forward(
          DeleteRecordFromShard(db,
                                namespace,
                                Location(location.coordinates, nodeName, location.from, location.to),
                                bit))
    case ExecuteDeleteStatementInternalInLocations(statement, schema, locations) =>
      getOrCreateAccumulator(statement.db, statement.namespace).forward(
        ExecuteDeleteStatementInShards(statement,
                                       schema,
                                       locations.map(l => Location(l.coordinates, nodeName, l.from, l.to))))
  }

}

object MetricsDataActorWrites {
  def props(basePath: String,
            nodeName: String,
            localCommitLogCoordinator: ActorRef,
            metricsReaderActor: ActorRef): Props =
    Props(new MetricsDataActorWrites(basePath, nodeName, localCommitLogCoordinator, metricsReaderActor))
}
