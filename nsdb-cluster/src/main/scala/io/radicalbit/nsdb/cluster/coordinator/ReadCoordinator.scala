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

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.NsdbPerfLogger
import io.radicalbit.nsdb.cluster.PubSubTopics._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.GetLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsGot
import io.radicalbit.nsdb.common.NSDbNumericType
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{Expression, SelectSQLStatement}
import io.radicalbit.nsdb.index.NumericType
import io.radicalbit.nsdb.model.{Location, Schema, TimeRange}
import io.radicalbit.nsdb.post_proc.{applyOrderingWithLimit, _}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.statement._
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.util.PipeableFutureWithSideEffect._
import spire.implicits._
import spire.math.Interval

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

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
    val intervals = TimeRangeManager.extractTimeRange(expression)
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
  private def gatherNodeResults(
      statement: SelectSQLStatement,
      schema: Schema,
      uniqueLocationsByNode: Map[String, Seq[Location]],
      ranges: Seq[TimeRange] = Seq.empty)(postProcFun: Seq[Bit] => Seq[Bit]): Future[ExecuteSelectStatementResponse] = {
    log.debug("gathering node results for locations {}", uniqueLocationsByNode)
    Future
      .sequence(metricsDataActors.map {
        case (nodeName, actor) =>
          actor ? ExecuteSelectStatement(statement,
                                         schema,
                                         uniqueLocationsByNode.getOrElse(nodeName, Seq.empty),
                                         ranges)
      })
      .map { rawResponses =>
        log.debug("gathered {} from locations {}", rawResponses, uniqueLocationsByNode)
        val errs = rawResponses.collect { case a: SelectStatementFailed => a }
        if (errs.nonEmpty) {
          SelectStatementFailed(statement, errs.map(_.reason).mkString(","))
        } else
          SelectStatementExecuted(
            statement,
            postProcFun(rawResponses.asInstanceOf[Seq[SelectStatementExecuted]].flatMap(_.values)))
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
        seq.groupBy(_.tags(groupBy)).map(m => aggregationFunction(m._2)).toSeq
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
            val filteredLocations = filterLocationsThroughTime(statement.condition.map(_.expression), locations)

            val uniqueLocationsByNode =
              filteredLocations.groupBy(l => (l.from, l.to)).map(_._2.head).toSeq.groupBy(_.node)

            StatementParser.parseStatement(statement, schema) match {
              //pure count(*) query
              case Right(_ @ParsedSimpleQuery(_, _, _, false, limit, fields, _))
                  if fields.lengthCompare(1) == 0 && fields.head.count =>
                gatherNodeResults(statement, schema, uniqueLocationsByNode)(seq => {
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
                })

              case Right(ParsedSimpleQuery(_, _, _, false, _, _, _)) =>
                gatherNodeResults(statement, schema, uniqueLocationsByNode)(
                  applyOrderingWithLimit(_, statement, schema))

              case Right(ParsedSimpleQuery(_, _, _, true, _, fields, _)) if fields.lengthCompare(1) == 0 =>
                val distinctField = fields.head.name

                gatherAndGroupNodeResults(statement, distinctField, schema, uniqueLocationsByNode) { values =>
                  Bit(
                    timestamp = 0,
                    value = NSDbNumericType(0),
                    dimensions = retrieveField(values, distinctField, (bit: Bit) => bit.dimensions),
                    tags = retrieveField(values, distinctField, (bit: Bit) => bit.tags)
                  )
                }

              case Right(ParsedAggregatedQuery(_, _, _, agg @ InternalCountSimpleAggregation(_, _), _, _)) =>
                gatherAndGroupNodeResults(statement, statement.groupBy.get.dimension, schema, uniqueLocationsByNode) {
                  values =>
                    Bit(0,
                        NSDbNumericType(values.map(_.value.rawValue.asInstanceOf[Long]).sum),
                        values.head.dimensions,
                        values.head.tags)
                }.map(limitAndOrder(_, statement, schema, Some(agg)))

              case Right(ParsedAggregatedQuery(_, _, _, aggregationType, _, _)) =>
                gatherAndGroupNodeResults(statement, statement.groupBy.get.dimension, schema, uniqueLocationsByNode) {
                  values =>
                    val v                = schema.fieldsMap("value").indexType.asInstanceOf[NumericType[_]]
                    implicit val numeric = v.numeric
                    aggregationType match {
                      case InternalMaxSimpleAggregation(_, _) =>
                        Bit(0,
                            NSDbNumericType(values.map(_.value.rawValue).max),
                            values.head.dimensions,
                            values.head.tags)
                      case InternalMinSimpleAggregation(_, _) =>
                        Bit(0,
                            NSDbNumericType(values.map(_.value.rawValue).min),
                            values.head.dimensions,
                            values.head.tags)
                      case InternalSumSimpleAggregation(_, _) =>
                        Bit(0,
                            NSDbNumericType(values.map(_.value.rawValue).sum),
                            values.head.dimensions,
                            values.head.tags)
                    }
                }.map(limitAndOrder(_, statement, schema))
              case Right(
                  ParsedTemporalAggregatedQuery(_,
                                                _,
                                                _,
                                                rangeLength,
                                                agg @ InternalCountTemporalAggregation,
                                                condition,
                                                _,
                                                _)) =>
                val sortedLocations = filteredLocations.sortBy(_.from)

                val globalRanges: Seq[TimeRange] =
                  if (sortedLocations.isEmpty) Seq.empty
                  else
                    TimeRangeManager.computeRangesForIntervalAndCondition(sortedLocations.last.to,
                                                                          sortedLocations.head.from,
                                                                          rangeLength,
                                                                          condition)

                gatherNodeResults(statement, schema, uniqueLocationsByNode, globalRanges) { res =>
                  applyOrderingWithLimit(
                    res
                      .groupBy(_.timestamp)
                      .mapValues(
                        v =>
                          Bit(v.head.timestamp,
                              NSDbNumericType(v.map(_.value.rawValue.asInstanceOf[Long]).sum),
                              v.head.dimensions,
                              v.head.tags))
                      .values
                      .toSeq,
                    statement,
                    schema,
                    Some(agg)
                  )
                }
              case Right(
                  ParsedTemporalAggregatedQuery(_,
                                                _,
                                                _,
                                                rangeLength,
                                                agg @ InternalSumTemporalAggregation,
                                                condition,
                                                _,
                                                _)) =>
                val sortedLocations = filteredLocations.sortBy(_.from)

                val globalRanges: Seq[TimeRange] =
                  TimeRangeManager.computeRangesForIntervalAndCondition(sortedLocations.last.to,
                                                                        sortedLocations.head.from,
                                                                        rangeLength,
                                                                        condition)

                gatherNodeResults(statement, schema, uniqueLocationsByNode, globalRanges) { res =>
                  val v                              = schema.fieldsMap("value").indexType.asInstanceOf[NumericType[_]]
                  implicit val numeric: Numeric[Any] = v.numeric
                  applyOrderingWithLimit(
                    res
                      .groupBy(_.timestamp)
                      .mapValues(
                        v =>
                          Bit(v.head.timestamp,
                              NSDbNumericType(v.map(_.value.rawValue).sum),
                              v.head.dimensions,
                              v.head.tags))
                      .values
                      .toSeq,
                    statement,
                    schema,
                    Some(agg)
                  )
                }
              case Right(
                  ParsedTemporalAggregatedQuery(_,
                                                _,
                                                _,
                                                rangeLength,
                                                aggregationType, //min or max
                                                condition,
                                                _,
                                                _)) =>
                val sortedLocations = filteredLocations.sortBy(_.from)

                val globalRanges: Seq[TimeRange] =
                  TimeRangeManager.computeRangesForIntervalAndCondition(sortedLocations.last.to,
                                                                        sortedLocations.head.from,
                                                                        rangeLength,
                                                                        condition)

                gatherNodeResults(statement, schema, uniqueLocationsByNode, globalRanges) { res =>
                  val v                              = schema.fieldsMap("value").indexType.asInstanceOf[NumericType[_]]
                  implicit val numeric: Numeric[Any] = v.numeric
                  applyOrderingWithLimit(
                    res
                      .groupBy(_.timestamp)
                      .mapValues(
                        v =>
                          if (aggregationType == InternalMaxTemporalAggregation)
                            Bit(v.head.timestamp,
                                NSDbNumericType(v.map(_.value.rawValue).max),
                                v.head.dimensions,
                                v.head.tags)
                          else {
                            val nonZeroValues: Seq[Any] =
                              v.collect { case x if x.value.rawValue != numeric.zero => x.value.rawValue }
                            Bit(v.head.timestamp,
                                NSDbNumericType(if (nonZeroValues.isEmpty) numeric.zero else nonZeroValues.min),
                                v.head.dimensions,
                                v.head.tags)
                        })
                      .values
                      .toSeq,
                    statement,
                    schema,
                    Some(aggregationType)
                  )
                }
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

  def props(metadataCoordinator: ActorRef, schemaActor: ActorRef, mediator: ActorRef): Props =
    Props(new ReadCoordinator(metadataCoordinator, schemaActor, mediator: ActorRef))

}
