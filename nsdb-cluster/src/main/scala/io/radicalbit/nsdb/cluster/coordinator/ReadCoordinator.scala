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
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.GetLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsGot
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{Expression, SelectSQLStatement}
import io.radicalbit.nsdb.index.NumericType
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.post_proc.{applyOrderingWithLimit, _}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.statement.{StatementParser, TimeRangeExtractor}
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._
import spire.implicits._
import spire.math.Interval

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  * Actor that receives and handles every read request.
  * @param metadataCoordinator  [[MetadataCoordinator]] the metadata coordinator.
  * @param schemaCoordinator [[SchemaCoordinator]] the metrics schema actor.
  */
class ReadCoordinator(metadataCoordinator: ActorRef, schemaCoordinator: ActorRef, mediator: ActorRef)
    extends ActorPathLogging
    with NsdbPerfLogger {

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.read-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  import context.dispatcher

  private val metricsDataActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  override def receive: Receive = warmUp

  override def preStart(): Unit = {

    mediator ! Subscribe(COORDINATORS_TOPIC, self)

    val interval = FiniteDuration(
      context.system.settings.config.getDuration("nsdb.publisher.scheduler.interval", TimeUnit.SECONDS),
      TimeUnit.SECONDS)

    /**
      * scheduler that updates aggregated queries subscribers
      */
    context.system.scheduler.schedule(FiniteDuration(0, "ms"), interval) {
      mediator ! Publish(NODE_GUARDIANS_TOPIC, GetMetricsDataActors)
      log.debug("readcoordinator data actor : {}", metricsDataActors.size)
    }
  }

  private def filterLocationsThroughTime(expression: Option[Expression], locations: Seq[Location]): Seq[Location] = {
    val intervals = TimeRangeExtractor.extractTimeRange(expression)
    locations.filter {
      case key if intervals.nonEmpty =>
        intervals
          .map(i => Interval.closed(key.from, key.to).intersect(i) != Interval.empty[Long])
          .foldLeft(false)((x, y) => x || y)
      case _ => true
    }
  }

  /**
    * Gathers results from every shard actor and elaborate them.
    * @param statement the Sql statement to be executed againt every actor.
    * @param schema Metric's schema.
    * @param postProcFun The function that will be applied after data are retrieved from all the shards.
    * @return the processed results.
    */
  private def gatherNodeResults(statement: SelectSQLStatement,
                                schema: Schema,
                                uniqueLocationsByNode: Map[String, Seq[Location]])(
      postProcFun: Seq[Bit] => Seq[Bit]): Future[Either[SelectStatementFailed, Seq[Bit]]] = {
    Future
      .sequence(metricsDataActors.map {
        case (nodeName, actor) =>
          actor ? ExecuteSelectStatement(statement, schema, uniqueLocationsByNode.getOrElse(nodeName, Seq.empty))
      })
      .map { e =>
        val errs = e.collect { case a: SelectStatementFailed => a }
        if (errs.nonEmpty) {
          Left(SelectStatementFailed(errs.map(_.reason).mkString(",")))
        } else
          Right(postProcFun(e.asInstanceOf[Seq[SelectStatementExecuted]].flatMap(_.values)))
      }
      .recover {
        case t =>
          log.error(t, "an error occurred while gathering results from nodes")
          Left(SelectStatementFailed(t.getMessage))
      }
  }

  /**
    * Groups results coming from different nodes according to the group by clause provided in the query.
    * @param statement the Sql statement to be executed against every actor.
    * @param groupBy the group by clause dimension.
    * @param schema Metric's schema.
    * @param aggregationFunction the aggregate function corresponding to the aggregation operator (sum, count ecc.) contained in the query.
    * @return the grouped results.
    */
  private def gatherAndGroupNodeResults(statement: SelectSQLStatement,
                                        groupBy: String,
                                        schema: Schema,
                                        uniqueLocationsByNode: Map[String, Seq[Location]])(
      aggregationFunction: Seq[Bit] => Bit): Future[Either[SelectStatementFailed, Seq[Bit]]] =
    gatherNodeResults(statement, schema, uniqueLocationsByNode) {
      case seq if uniqueLocationsByNode.size > 1 =>
        seq.groupBy(_.tags(groupBy)).map(m => aggregationFunction(m._2)).toSeq
      case seq => seq
    }

  /**
    * Initial state in which actor waits metadata warm-up completion.
    */
  def warmUp: Receive = {
    case WarmUpCompleted =>
      context.become(operating)
    case SubscribeMetricsDataActor(actor: ActorRef, nodeName) =>
      if (!metricsDataActors.get(nodeName).contains(actor)) {
        metricsDataActors += (nodeName -> actor)
        log.info(s"subscribed data actor for node $nodeName")
      }
      sender() ! MetricsDataActorSubscribed(actor, nodeName)
    case GetConnectedDataNodes =>
      sender ! ConnectedDataNodesGot(metricsDataActors.keys.toSeq)
    case msq =>
      // Requests received during warm-up are ignored, this results in a timeout
      log.error(s"Received ignored message $msq during warmUp")
  }

  def operating: Receive = {
    case SubscribeMetricsDataActor(actor: ActorRef, nodeName) =>
      if (!metricsDataActors.get(nodeName).contains(actor)) {
        metricsDataActors += (nodeName -> actor)
        log.info(s"subscribed data actor for node $nodeName")
      }
      sender() ! MetricsDataActorSubscribed(actor, nodeName)
    case GetConnectedDataNodes =>
      sender ! ConnectedDataNodesGot(metricsDataActors.keys.toSeq)
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
      schemaCoordinator forward msg
    case ExecuteStatement(statement) =>
      val startTime = System.currentTimeMillis()
      log.debug("executing {} with {} data actors", statement, metricsDataActors.size)
      Future
        .sequence(Seq(
          schemaCoordinator ? GetSchema(statement.db, statement.namespace, statement.metric),
          metadataCoordinator ? GetLocations(statement.db, statement.namespace, statement.metric)
        ))
        .flatMap {
          case SchemaGot(_, _, _, Some(schema)) :: LocationsGot(_, _, _, locations) :: Nil =>
            val filteredLocations = filterLocationsThroughTime(statement.condition.map(_.expression), locations)

            val uniqueLocationsByNode =
              filteredLocations.groupBy(l => (l.from, l.to)).map(_._2.head).toSeq.groupBy(_.node)

            val x = StatementParser.parseStatement(statement, schema) match {
              //pure count(*) query
              case Success(_ @ParsedSimpleQuery(_, _, _, false, limit, fields, _))
                  if fields.lengthCompare(1) == 0 && fields.head.count =>
                gatherNodeResults(statement, schema, uniqueLocationsByNode)(seq => {
                  val recordCount = seq.map(_.value.asInstanceOf[Int]).sum
                  val count       = if (recordCount <= limit) recordCount else limit

                  Seq(
                    Bit(timestamp = 0,
                        value = count,
                        dimensions = retrieveCount(seq, count, (bit: Bit) => bit.dimensions),
                        tags = retrieveCount(seq, count, (bit: Bit) => bit.tags)))
                })

              case Success(_ @ParsedSimpleQuery(_, _, _, false, limit, fields, _)) =>
                gatherNodeResults(statement, schema, uniqueLocationsByNode)(identity)

              case Success(ParsedSimpleQuery(_, _, _, true, _, fields, _)) if fields.lengthCompare(1) == 0 =>
                val distinctField = fields.head.name

                gatherAndGroupNodeResults(statement, distinctField, schema, uniqueLocationsByNode) { values =>
                  Bit(
                    timestamp = 0,
                    value = 0,
                    dimensions = retrieveField(values, distinctField, (bit: Bit) => bit.dimensions),
                    tags = retrieveField(values, distinctField, (bit: Bit) => bit.tags)
                  )
                }

              case Success(ParsedAggregatedQuery(_, _, _, InternalCountAggregation(_, _), _, _)) =>
                gatherAndGroupNodeResults(statement, statement.groupBy.get, schema, uniqueLocationsByNode) { values =>
                  Bit(0, values.map(_.value.asInstanceOf[Long]).sum, values.head.dimensions, values.head.tags)
                }

              case Success(ParsedAggregatedQuery(_, _, _, aggregationType, _, _)) =>
                gatherAndGroupNodeResults(statement, statement.groupBy.get, schema, uniqueLocationsByNode) { values =>
                  val v                                        = schema.fields.find(_.name == "value").get.indexType.asInstanceOf[NumericType[_, _]]
                  implicit val numeric: Numeric[JSerializable] = v.numeric
                  aggregationType match {
                    case InternalMaxAggregation(_, _) =>
                      Bit(0, values.map(_.value).max, values.head.dimensions, values.head.tags)
                    case InternalMinAggregation(_, _) =>
                      Bit(0, values.map(_.value).min, values.head.dimensions, values.head.tags)
                    case InternalSumAggregation(_, _) =>
                      Bit(0, values.map(_.value).sum, values.head.dimensions, values.head.tags)
                  }
                }
              case Failure(ex) =>
                Future(Left(SelectStatementFailed("Select Statement not valid")))
              case _ =>
                Future(Left(SelectStatementFailed("Not a select statement.")))
            }

            applyOrderingWithLimit(x, statement, schema).map {
              case Right(results) =>
                SelectStatementExecuted(statement.db, statement.namespace, statement.metric, results)
              case Left(failed) => failed
            }
          case _ =>
            Future(
              SelectStatementFailed(s"Metric ${statement.metric} does not exist ", MetricNotFound(statement.metric)))
        }
        .recoverWith {
          case t =>
            log.error(t, "")
            Future(SelectStatementFailed("Generic error occurred"))
        }
        .pipeToWithEffect(sender()) { _ =>
          if (perfLogger.isDebugEnabled)
            perfLogger.debug("executed statement {} in {} millis", statement, System.currentTimeMillis() - startTime)
        }
  }
}

object ReadCoordinator {

  def props(metadataCoordinator: ActorRef, schemaActor: ActorRef, mediator: ActorRef): Props =
    Props(new ReadCoordinator(metadataCoordinator, schemaActor, mediator: ActorRef))

}
