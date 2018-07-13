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

package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._

import scala.collection.mutable
import scala.concurrent.Future

/**
  * Actor that receives and handles every read request.
  * @param metadataCoordinator  [[MetadataCoordinator]] the metadata coordinator.
  * @param namespaceSchemaActor [[io.radicalbit.nsdb.cluster.actor.MetricsSchemaActor]] the metrics schema actor.
  */
class ReadCoordinator(metadataCoordinator: ActorRef, namespaceSchemaActor: ActorRef)
    extends ActorPathLogging
    with NsdbPerfLogger {

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.read-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  import context.dispatcher

  private val metricsDataActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  override def receive: Receive = warmUp

  /**
    * Initial state in which actor waits metadata warm-up completion.
    */
  def warmUp: Receive = {
    case WarmUpCompleted =>
      context.become(operating)
    case SubscribeMetricsDataActor(actor: ActorRef, nodeName) =>
      metricsDataActors += (nodeName -> actor)
      sender() ! MetricsDataActorSubscribed(actor, nodeName)
    case GetConnectedNodes =>
      sender ! ConnectedNodesGot(metricsDataActors.keys.toSeq)
    case msq =>
      // Requests received during warm-up are ignored, this results in a timeout
      log.error(s"Received ignored message $msq during warmUp")
  }

  def operating: Receive = {
    case SubscribeMetricsDataActor(actor: ActorRef, nodeName) =>
      metricsDataActors += (nodeName -> actor)
      sender() ! MetricsDataActorSubscribed(actor, nodeName)
    case GetConnectedNodes =>
      sender ! ConnectedNodesGot(metricsDataActors.keys.toSeq)
    case msg @ GetDbs =>
      Future
        .sequence(metricsDataActors.values.toSeq.map(actor => (actor ? msg).mapTo[DbsGot].map(_.dbs)))
        .map(_.flatten.toSet)
        .map(dbs => DbsGot(dbs))
        .pipeTo(sender)
    case msg @ GetNamespaces(db) =>
      Future
        .sequence(metricsDataActors.values.toSeq.map(actor => (actor ? msg).mapTo[NamespacesGot].map(_.namespaces)))
        .map(_.flatten.toSet)
        .map(namespaces => NamespacesGot(db, namespaces))
        .pipeTo(sender)
    case msg @ GetMetrics(db, namespace) =>
      Future
        .sequence(metricsDataActors.values.toSeq.map(actor => (actor ? msg).mapTo[MetricsGot].map(_.metrics)))
        .map(_.flatten.toSet)
        .map(metrics => MetricsGot(db, namespace, metrics))
        .pipeTo(sender)
    case msg: GetSchema =>
      namespaceSchemaActor forward msg
    case ExecuteStatement(statement) =>
      val startTime = System.currentTimeMillis()
      log.debug("executing {} ", statement)
      (namespaceSchemaActor ? GetSchema(statement.db, statement.namespace, statement.metric))
        .flatMap {
          case SchemaGot(_, _, _, Some(schema)) =>
            Future
              .sequence(metricsDataActors.values.toSeq.map(actor => actor ? ExecuteSelectStatement(statement, schema)))
              .map { seq =>
                val errs = seq.collect {
                  case e: SelectStatementFailed => e.reason
                }
                if (errs.isEmpty) {
                  val results = seq.asInstanceOf[Seq[SelectStatementExecuted]]
                  SelectStatementExecuted(statement.db,
                                          statement.namespace,
                                          statement.metric,
                                          results.flatMap(_.values))
                } else {
                  SelectStatementFailed(errs.mkString(","))
                }
              }
          case _ =>
            Future(
              SelectStatementFailed(s"Metric ${statement.metric} does not exist ", MetricNotFound(statement.metric)))
        }
        .recoverWith {
          case t => Future(SelectStatementFailed(t.getMessage))
        }
        .pipeToWithEffect(sender()) { _ =>
          if (perfLogger.isDebugEnabled)
            perfLogger.debug("executed statement {} in {} millis", statement, System.currentTimeMillis() - startTime)
        }
  }
}

object ReadCoordinator {

  def props(metadataCoordinator: ActorRef, schemaActor: ActorRef): Props =
    Props(new ReadCoordinator(metadataCoordinator, schemaActor))

}
