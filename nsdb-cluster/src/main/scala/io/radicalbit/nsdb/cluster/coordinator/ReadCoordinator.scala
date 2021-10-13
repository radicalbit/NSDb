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

package io.radicalbit.nsdb.cluster.coordinator

import java.math.{MathContext, RoundingMode}
import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.logic.ReadNodesSelection
import io.radicalbit.nsdb.common.NSDbNumericType
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel.precision
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.{Location, Schema, TimeContext, TimeRangeContext}
import io.radicalbit.nsdb.post_proc._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.statement._
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * Actor that receives and handles every read request.
  * @param metadataCoordinator  [[MetadataCoordinator]] the metadata coordinator.
  * @param schemaCoordinator [[SchemaCoordinator]] the metrics schema actor.
  */
class ReadCoordinator(metadataCoordinator: ActorRef,
                      schemaCoordinator: ActorRef,
                      mediator: ActorRef,
                      readNodesSelection: ReadNodesSelection)
    extends ActorPathLogging
    with NsdbPerfLogger {

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.read-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  implicit val mathContext: MathContext =
    new MathContext(context.system.settings.config.getInt(precision), RoundingMode.HALF_UP)

  import context.dispatcher

  private val metricsDataActors: mutable.Map[String, ActorRef] = mutable.Map.empty

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

  /**
    * Gathers results from every shard actor and elaborate them.
    * @param command the command that contains the Sql statement to be executed against every actor.
    * @param schema Metric schema.
    * @param postProcFun The function that will be applied after data are retrieved from all the shards.
    * @param timeContext the timeContext that will be propagated to all the nodes.
    * @return the processed results.
    */
  private def gatherNodeResults(command: ExecuteStatement,
                                schema: Schema,
                                uniqueLocationsByNode: Map[String, Seq[Location]],
                                timeRangeContext: Option[TimeRangeContext] = None)(postProcFun: Seq[Bit] => Seq[Bit])(
      implicit timeContext: TimeContext): Future[ExecuteSelectStatementResponse] = {
    log.debug("gathering node results for locations {}", uniqueLocationsByNode)
    val statement    = command.selectStatement
    val isSingleNode = uniqueLocationsByNode.keys.size == 1

    Future
      .sequence(metricsDataActors.collect {
        case (uniqueNodeId, actor) if uniqueLocationsByNode.isDefinedAt(uniqueNodeId) =>
          actor ? ExecuteSelectStatement(statement,
                                         schema,
                                         uniqueLocationsByNode.getOrElse(uniqueNodeId, Seq.empty),
                                         timeRangeContext,
                                         timeContext,
                                         isSingleNode)
      })
      .map { rawResponses =>
        log.debug("gathered {} from locations {}", rawResponses, uniqueLocationsByNode)
        val errs = rawResponses.collect { case a: SelectStatementFailed => a }
        if (errs.nonEmpty) {
          SelectStatementFailed(statement, errs.map(_.reason).mkString(","))
        } else {
          val combinedResponsesFromNodes =
            rawResponses.asInstanceOf[Seq[SelectStatementExecuted]].flatMap(_.values)
          log.debug(s"combined results from node $combinedResponsesFromNodes")
          SelectStatementExecuted(
            statement,
            if (isSingleNode) combinedResponsesFromNodes
            else postProcFun(combinedResponsesFromNodes),
            schema
          )
        }
      }
      .recover {
        case t =>
          log.error(t, "an error occurred while gathering results from nodes")
          SelectStatementFailed(statement, t.getMessage)
      }
  }

  /**
    * Groups results coming from different nodes according to the group by clause provided in the query.
    * @param command the command that contains the Sql statement to be executed against every actor.
    * @param groupBy the group by clause dimension.
    * @param schema Metric schema.
    * @param aggregationFunction the aggregate function corresponding to the aggregation operator (sum, count ecc.) contained in the query.
    * @param timeContext the timeContext that will be propagated to all the nodes.
    * @return the grouped results.
    */
  private def gatherAndGroupNodeResults(command: ExecuteStatement,
                                        groupBy: String,
                                        schema: Schema,
                                        uniqueLocationsByNode: Map[String, Seq[Location]])(
      aggregationFunction: Seq[Bit] => Bit)(implicit timeContext: TimeContext): Future[ExecuteSelectStatementResponse] =
    gatherNodeResults(command, schema, uniqueLocationsByNode) {
      case seq if uniqueLocationsByNode.size > 1 =>
        seq.groupBy(_.tags(groupBy)).mapValues(aggregationFunction).values.toSeq
      case seq => seq
    }

  override def receive: Receive = {
    case SubscribeMetricsDataActor(actor, nodeId) =>
      if (!metricsDataActors.get(nodeId).contains(actor)) {
        metricsDataActors += (nodeId -> actor)
        log.info(s"subscribed data actor for node $nodeId")
      }
      sender ! MetricsDataActorSubscribed(actor, nodeId)
    case UnsubscribeMetricsDataActor(nodeId) =>
      metricsDataActors -= nodeId
      log.info(s"metric data actor removed for node $nodeId")
      sender ! MetricsDataActorUnSubscribed(nodeId)
    case GetDbs =>
      metadataCoordinator forward GetDbs
    case msg @ GetNamespaces(_) =>
      metadataCoordinator forward msg
    case msg @ GetMetrics(_, _) =>
      metadataCoordinator forward msg
    case msg: GetSchema =>
      schemaCoordinator forward msg
    case ValidateStatement(statement, context) =>
      log.debug("validating {}", statement)
      implicit val timeContext: TimeContext = context.getOrElse(TimeContext())
      (schemaCoordinator ? GetSchema(statement.db, statement.namespace, statement.metric))
        .map {
          case SchemaGot(_, _, _, Some(schema)) =>
            StatementParser.parseStatement(statement, schema) match {
              case Right(_)  => SelectStatementValidated(statement)
              case Left(err) => SelectStatementValidationFailed(statement, err)
            }
          case _ =>
            SelectStatementValidationFailed(statement,
                                            s"metric ${statement.metric} does not exist",
                                            MetricNotFound(statement.metric))
        }
        .pipeTo(sender())
    case msg @ ExecuteStatement(statement, context) =>
      val startTime                         = System.currentTimeMillis()
      implicit val timeContext: TimeContext = context.getOrElse(TimeContext())
      log.debug("executing {} with {} data actors", statement, metricsDataActors.size)

      val selectStatementResponse: Future[ExecuteSelectStatementResponse] =
        (for {
          getSchemaResponse <- schemaCoordinator ? GetSchema(statement.db, statement.namespace, statement.metric)
          liveLocationResponse <- metadataCoordinator ? GetLiveLocations(statement.db,
                                                                         statement.namespace,
                                                                         statement.metric)
        } yield (getSchemaResponse, liveLocationResponse))
          .recoverWith {
            case t =>
              log.error(t, s"Error in Execute Statement $statement")
              Future(SelectStatementFailed(statement, "Generic error occurred"))
          }
          .flatMap {
            case (SchemaGot(_, _, _, Some(schema)), LiveLocationsGot(_, _, _, liveLocations)) =>
              log.debug(s"found schema $schema, and live $liveLocations")
              val filteredLocations: Seq[Location] =
                ReadNodesSelection.filterLocationsThroughTime(statement.condition.map(_.expression), liveLocations)

              val uniqueLocationsByNode: Map[String, Seq[Location]] =
                readNodesSelection.getDistinctLocationsByNode(filteredLocations)

              StatementParser.parseStatement(statement, schema) match {
                case Right(ParsedSimpleQuery(_, _, _, _, false, _, _, _)) =>
                  gatherNodeResults(msg, schema, uniqueLocationsByNode)(applyOrderingWithLimit(_, statement, schema))

                case Right(ParsedSimpleQuery(_, _, _, _, true, _, fields, _)) if fields.lengthCompare(1) == 0 =>
                  val distinctField = fields.head.name

                  gatherAndGroupNodeResults(msg, distinctField, schema, uniqueLocationsByNode) { bits =>
                    Bit(
                      timestamp = 0,
                      value = NSDbNumericType(0),
                      dimensions = retrieveField(bits, distinctField, (bit: Bit) => bit.dimensions),
                      tags = retrieveField(bits, distinctField, (bit: Bit) => bit.tags)
                    )
                  }.map(limitAndOrder(_))

                case Right(ParsedGlobalAggregatedQuery(_, _, _, _, _, fields, aggregations, _)) =>
                  gatherNodeResults(msg, schema, uniqueLocationsByNode) { rawResults =>
                    globalAggregationReduce(rawResults, fields, aggregations, statement, schema)
                  }
                case Right(ParsedAggregatedQuery(_, _, _, _, aggregation, _, _)) =>
                  gatherAndGroupNodeResults(msg, statement.groupBy.get.field, schema, uniqueLocationsByNode)(
                    internalAggregationReduce(_, schema, aggregation)
                  ).map(limitAndOrder(_, Some(aggregation)))

                case Right(
                    ParsedTemporalAggregatedQuery(_,
                                                  _,
                                                  _,
                                                  _,
                                                  rangeLength,
                                                  aggregation,
                                                  condition,
                                                  _,
                                                  gracePeriod,
                                                  _)) =>
                  val sortedLocations = filteredLocations.sortBy(_.from)
                  val limitedLocations = gracePeriod.fold(sortedLocations)(gracePeriodInterval =>
                    ReadNodesSelection.filterLocationsThroughGracePeriod(gracePeriodInterval, sortedLocations))

                  val timeRangeContext: Option[TimeRangeContext] =
                    if (limitedLocations.isEmpty) None
                    else {

                      val upperBound = limitedLocations.last.to
                      val lowerBound = limitedLocations.head.from

                      Some(
                        TimeRangeContext(upperBound,
                                         lowerBound,
                                         rangeLength,
                                         TimeRangeManager.computeRangesForIntervalAndCondition(upperBound,
                                                                                               lowerBound,
                                                                                               rangeLength,
                                                                                               condition,
                                                                                               gracePeriod)))
                    }

                  val limitedUniqueLocationsByNode = readNodesSelection.getDistinctLocationsByNode(limitedLocations)

                  gatherNodeResults(msg, schema, limitedUniqueLocationsByNode, timeRangeContext)(
                    postProcessingTemporalQueryResult(schema, statement, aggregation))

                case Left(error) =>
                  Future(SelectStatementFailed(statement, error))
                case _ =>
                  Future(SelectStatementFailed(statement, "Not a select statement."))
              }
            case _ =>
              Future(
                SelectStatementFailed(statement,
                                      s"Metric ${statement.metric} does not exist ",
                                      MetricNotFound(statement.metric)))
          }

      selectStatementResponse.pipeToWithEffect(sender()) { _ =>
        if (perfLogger.isDebugEnabled)
          perfLogger.debug("executed statement {} in {} millis", statement, System.currentTimeMillis() - startTime)
      }
  }
}

object ReadCoordinator {

  def props(metadataCoordinator: ActorRef,
            schemaActor: ActorRef,
            mediator: ActorRef,
            readNodesSelection: ReadNodesSelection): Props =
    Props(new ReadCoordinator(metadataCoordinator, schemaActor, mediator: ActorRef, readNodesSelection))

}
