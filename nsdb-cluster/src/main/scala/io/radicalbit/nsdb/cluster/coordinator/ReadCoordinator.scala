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
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.GetLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsGot
import io.radicalbit.nsdb.cluster.logic.ReadNodesSelection
import io.radicalbit.nsdb.common.NSDbNumericType
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel.precision
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.model.{Location, Schema, TimeRange}
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
    * @param statement the Sql statement to be executed against every actor.
    * @param schema Metric's schema.
    * @param postProcFun The function that will be applied after data are retrieved from all the shards.
    * @return the processed results.
    */
  private def gatherNodeResults(
      statement: SelectSQLStatement,
      schema: Schema,
      uniqueLocationsByNode: Map[String, Seq[Location]],
      ranges: Seq[TimeRange] = Seq.empty)(postProcFun: Seq[Bit] => Seq[Bit]): Future[ExecuteSelectStatementResponse] = {
    log.debug("gathering node results for locations {}", uniqueLocationsByNode)

    val isSingleNode = uniqueLocationsByNode.keys.size == 1

    log.debug(s"isSingleNode: $isSingleNode")

    Future
      .sequence(metricsDataActors.collect {
        case (nodeName, actor) if uniqueLocationsByNode.isDefinedAt(nodeName) =>
          actor ? ExecuteSelectStatement(statement,
                                         schema,
                                         uniqueLocationsByNode.getOrElse(nodeName, Seq.empty),
                                         ranges,
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
          SelectStatementExecuted(
            statement,
            if (isSingleNode) combinedResponsesFromNodes
            else postProcFun(combinedResponsesFromNodes)
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
      aggregationFunction: Seq[Bit] => Bit): Future[ExecuteSelectStatementResponse] =
    gatherNodeResults(statement, schema, uniqueLocationsByNode) {
      case seq if uniqueLocationsByNode.size > 1 =>
        seq.groupBy(_.tags(groupBy)).mapValues(aggregationFunction).values.toSeq
      case seq => seq
    }

  override def receive: Receive = {
    case SubscribeMetricsDataActor(actor: ActorRef, nodeName) =>
      if (!metricsDataActors.get(nodeName).contains(actor)) {
        metricsDataActors += (nodeName -> actor)
        log.info(s"subscribed data actor for node $nodeName")
      }
      sender ! MetricsDataActorSubscribed(actor, nodeName)
    case UnsubscribeMetricsDataActor(nodeName) =>
      metricsDataActors -= nodeName
      log.info(s"metric data actor removed for node $nodeName")
      sender ! MetricsDataActorUnSubscribed(nodeName)
    case GetConnectedDataNodes =>
      sender ! ConnectedDataNodesGot(metricsDataActors.keys.toSeq)
    case GetDbs =>
      metadataCoordinator forward GetDbs
    case msg @ GetNamespaces(_) =>
      metadataCoordinator forward msg
    case msg @ GetMetrics(_, _) =>
      metadataCoordinator forward msg
    case msg: GetSchema =>
      schemaCoordinator forward msg
    case ValidateStatement(statement) =>
      log.debug("validating {}", statement)
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
    case ExecuteStatement(statement) =>
      val startTime = System.currentTimeMillis()
      log.debug("executing {} with {} data actors", statement, metricsDataActors.size)
      val selectStatementResponse: Future[ExecuteSelectStatementResponse] = Future
        .sequence(
          Seq(
            schemaCoordinator ? GetSchema(statement.db, statement.namespace, statement.metric),
            metadataCoordinator ? GetLocations(statement.db, statement.namespace, statement.metric)
          ))
        .flatMap {
          case SchemaGot(_, _, _, Some(schema)) :: LocationsGot(_, _, _, locations) :: Nil =>
            log.debug("found schema {} and locations", schema, locations)
            val filteredLocations: Seq[Location] =
              ReadNodesSelection.filterLocationsThroughTime(statement.condition.map(_.expression), locations)

            val uniqueLocationsByNode: Map[String, Seq[Location]] =
              readNodesSelection.getDistinctLocationsByNode(filteredLocations)

            StatementParser.parseStatement(statement, schema) match {
              //pure count(*) query
              case Right(ParsedSimpleQuery(_, _, _, false, limit, fields, _))
                  if fields.lengthCompare(1) == 0 && fields.head.count =>
                gatherNodeResults(statement, schema, uniqueLocationsByNode) { seq =>
                  val recordCount = seq.map(_.value.rawValue.asInstanceOf[Int]).sum
                  val count       = if (recordCount <= limit) recordCount else limit
                  applyOrderingWithLimit(
                    Seq(Bit(
                      timestamp = 0,
                      value = NSDbNumericType(count),
                      dimensions = retrieveCount(seq, count, (bit: Bit) => bit.dimensions),
                      tags = retrieveCount(seq, count, (bit: Bit) => bit.tags)
                    )),
                    statement,
                    schema
                  )
                }

              case Right(ParsedSimpleQuery(_, _, _, false, _, _, _)) =>
                gatherNodeResults(statement, schema, uniqueLocationsByNode)(
                  applyOrderingWithLimit(_, statement, schema))

              case Right(ParsedSimpleQuery(_, _, _, true, _, fields, _)) if fields.lengthCompare(1) == 0 =>
                val distinctField = fields.head.name

                gatherAndGroupNodeResults(statement, distinctField, schema, uniqueLocationsByNode) { bits =>
                  Bit(
                    timestamp = 0,
                    value = NSDbNumericType(0),
                    dimensions = retrieveField(bits, distinctField, (bit: Bit) => bit.dimensions),
                    tags = retrieveField(bits, distinctField, (bit: Bit) => bit.tags)
                  )
                }.map(limitAndOrder(_, statement, schema))
              case Right(ParsedAggregatedQuery(_, _, _, aggregation, _, _)) =>
                gatherAndGroupNodeResults(statement, statement.groupBy.get.field, schema, uniqueLocationsByNode)(
                  internalAggregationReduce(_, schema, aggregation)
                ).map(limitAndOrder(_, statement, schema, Some(aggregation)))

              case Right(ParsedTemporalAggregatedQuery(_, _, _, rangeLength, aggregation, condition, _, _)) =>
                val sortedLocations = filteredLocations.sortBy(_.from)

                val globalRanges: Seq[TimeRange] =
                  if (sortedLocations.isEmpty) Seq.empty[TimeRange]
                  else
                    TimeRangeManager.computeRangesForIntervalAndCondition(sortedLocations.last.to,
                                                                          sortedLocations.head.from,
                                                                          rangeLength,
                                                                          condition)

                gatherNodeResults(statement, schema, uniqueLocationsByNode, globalRanges)(
                  postProcessingTemporalQueryResult(schema, statement, aggregation))

              case Left(_) =>
                Future(SelectStatementFailed(statement, "Select Statement not valid"))
              case _ =>
                Future(SelectStatementFailed(statement, "Not a select statement."))
            }
          case _ =>
            Future(
              SelectStatementFailed(statement,
                                    s"Metric ${statement.metric} does not exist ",
                                    MetricNotFound(statement.metric)))
        }
        .recoverWith {
          case t =>
            log.error(t, s"Error in Execute Statement $statement")
            Future(SelectStatementFailed(statement, "Generic error occurred"))
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
