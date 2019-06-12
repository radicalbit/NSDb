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

package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.ShardReaderActor.{DeleteAll, RefreshShard}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType, ValueFieldType}
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, SelectSQLStatement}
import io.radicalbit.nsdb.index.NumericType
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.post_proc.{applyOrderingWithLimit, _}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.statement.StatementParser
import spire.implicits._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * Actor responsible for:
  *
  * - Retrieving data from shards, aggregates and returns it to the sender.
  *
  * @param basePath shards actors path.
  * @param nodeName String representation of the host and the port Actor is deployed at.
  * @param db shards db.
  * @param namespace shards namespace.
  */
class MetricReaderActor(val basePath: String, nodeName: String, val db: String, val namespace: String)
    extends Actor
    with ActorLogging {
  import scala.collection.mutable

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  private val actors: mutable.Map[Location, ActorRef] = mutable.Map.empty

  /**
    * Gets or creates the actor for a given shard key.
    *
    * @param location The shard key to identify the actor.
    * @return The existing or the created shard actor.
    */
  private def getOrCreateActor(location: Location) =
    actors.getOrElse(
      location, {
        val newActor = context.actorOf(ShardReaderActor.props(basePath, db, namespace, location), actorName(location))
        actors += (location -> newActor)
        newActor
      }
    )

  private def actorName(location: Location) =
    s"shard_reader_${location.node}_${location.metric}_${location.from}_${location.to}"

  /**
    * Retrieve all the shard actors for a given metric.
    *
    * @param metric The metric to filter shard actors
    * @return All the shard actors for a given metric.
    */
  private def actorsForMetric(metric: String): mutable.Map[Location, ActorRef] = actors.filter(_._1.metric == metric)

  /**
    * Filters all the shard actors of a metrics given a set of locations.
    *
    * @param locations locations to filter the shard actors with.
    * @return filtered map containing all the actors for the given locations.
    */
  private def actorsForLocations(locations: Seq[Location]): Seq[(Location, ActorRef)] =
    actors.filterKeys(locations.toSet).toSeq

  /**
    * Any existing shard is retrieved
    */
  override def preStart: Unit = {
    Option(Paths.get(basePath, db, namespace, "shards").toFile.list())
      .map(_.toSet)
      .getOrElse(Set.empty)
      .filter(_.split("_").length == 3)
      .map(_.split("_"))
      .foreach {
        case Array(metric, from, to) =>
          val key        = Location(metric, nodeName, from.toLong, to.toLong)
          val shardActor = context.actorOf(ShardReaderActor.props(basePath, db, namespace, key))
          actors += (key -> shardActor)
      }
  }

  /**
    * Groups results coming from different shards according to the group by clause provided in the query.
    *
    * @param statement the Sql statement to be executed against every actor.
    * @param groupBy the group by clause dimension.
    * @param schema Metric's schema.
    * @param aggregationFunction the aggregate function corresponding to the aggregation operator (sum, count ecc.) contained in the query.
    * @return the grouped results.
    */
  private def gatherAndGroupShardResults(
      actors: Seq[(Location, ActorRef)],
      statement: SelectSQLStatement,
      groupBy: String,
      schema: Schema)(aggregationFunction: Seq[Bit] => Bit): Future[Either[SelectStatementFailed, Seq[Bit]]] = {

    gatherShardResults(actors, statement, schema) { seq =>
      seq
        .groupBy(_.tags(groupBy))
        .map(m => aggregationFunction(m._2))
        .toSeq
    }
  }

  /**
    * Gathers results from every shard actor and elaborate them.
    *
    * @param actors Shard actors to retrieve results from.
    * @param statement the Sql statement to be executed againt every actor.
    * @param schema Metric's schema.
    * @param postProcFun The function that will be applied after data are retrieved from all the shards.
    * @return the processed results.
    */
  private def gatherShardResults(actors: Seq[(Location, ActorRef)], statement: SelectSQLStatement, schema: Schema)(
      postProcFun: Seq[Bit] => Seq[Bit]): Future[Either[SelectStatementFailed, Seq[Bit]]] = {
    Future
      .sequence(actors.map {
        case (_, actor) =>
          (actor ? ExecuteSelectStatement(statement, schema, actors.map(_._1)))
            .recoverWith { case t => Future(SelectStatementFailed(t.getMessage)) }
      })
      .map { e =>
        val errs = e.collect { case a: SelectStatementFailed => a }
        if (errs.nonEmpty) {
          Left(SelectStatementFailed(errs.map(_.reason).mkString(",")))
        } else
          Right(postProcFun(e.asInstanceOf[Seq[SelectStatementExecuted]].flatMap(_.values)))
      }
  }

  /**
    * Retrieves and order results from different shards in case the statement does not contains aggregations
    * and a where condition involving timestamp has been provided.
    *
    * @param statement raw statement.
    * @param parsedStatement parsed statement.
    * @param actors shard actors to retrieve data from.
    * @param schema metric's schema.
    * @return a single sequence of results obtained from different shards.
    */
  private def retrieveAndOrderPlainResults(statement: SelectSQLStatement,
                                           parsedStatement: ParsedSimpleQuery,
                                           actors: Seq[(Location, ActorRef)],
                                           schema: Schema): Future[Either[SelectStatementFailed, Seq[Bit]]] = {
    if (statement.getTimeOrdering.isDefined || statement.order.isEmpty) {

      val eventuallyOrderedActors =
        statement.getTimeOrdering.map(actors.sortBy(_._1.from)(_)).getOrElse(actors)

      gatherShardResults(eventuallyOrderedActors, statement, schema) { seq =>
        seq.take(parsedStatement.limit)
      }

    } else {

      gatherShardResults(actors, statement, schema) { seq =>
        val schemaField = schema.fields.find(_.name == statement.order.get.dimension).get
        val o           = schemaField.indexType.ord
        implicit val ord: Ordering[JSerializable] =
          if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse else o

        val sorted =
          if (schemaField.fieldClassType == DimensionFieldType)
            seq.sortBy(_.dimensions(statement.order.get.dimension))
          else if (schemaField.fieldClassType == ValueFieldType)
            seq.sortBy(_.value)
          else
            seq.sortBy(_.tags(statement.order.get.dimension))

        statement.limit.map(l => sorted.take(l.value)).getOrElse(sorted)
      }
    }
  }

  private def generateResponse(db: String,
                               namespace: String,
                               metric: String,
                               rawResp: Either[SelectStatementFailed, Seq[Bit]]) =
    rawResp match {
      case Right(seq) => SelectStatementExecuted(db, namespace, metric, seq)
      case Left(err)  => err
    }

  /**
    * behaviour for read operations.
    *
    * - [[GetMetrics]] retrieve and return all the metrics.
    *
    * - [[ExecuteSelectStatement]] execute a given sql statement.
    */
  def readOps: Receive = {
    case GetMetrics(_, _) =>
      sender() ! MetricsGot(db, namespace, actors.keys.map(_.metric).toSet)
    case msg @ GetCount(_, ns, metric) =>
      Future
        .sequence(actorsForMetric(metric).map {
          case (_, actor) =>
            (actor ? msg).mapTo[CountGot].map(_.count)
        })
        .map(s => CountGot(db, ns, metric, s.sum))
        .pipeTo(sender)

    case ExecuteSelectStatement(statement, schema, locations) =>
      StatementParser.parseStatement(statement, schema) match {
        case Success(parsedStatement @ ParsedSimpleQuery(_, _, _, false, limit, fields, _)) =>
          val actors =
            actorsForLocations(locations)

          val orderedResults = retrieveAndOrderPlainResults(statement, parsedStatement, actors, schema)

          orderedResults
            .map {
              case Right(seq) =>
                if (fields.lengthCompare(1) == 0 && fields.head.count) {
                  val recordCount = seq.map(_.value.asInstanceOf[Int]).sum
                  val count       = if (recordCount <= limit) recordCount else limit

                  val bits = Seq(
                    Bit(timestamp = 0,
                        value = count,
                        dimensions = retrieveCount(seq, count, (bit: Bit) => bit.dimensions),
                        tags = retrieveCount(seq, count, (bit: Bit) => bit.tags)))

                  SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
                } else {
                  SelectStatementExecuted(
                    statement.db,
                    statement.namespace,
                    statement.metric,
                    seq.map(
                      b =>
                        if (b.tags.contains("count(*)"))
                          b.copy(tags = b.tags + ("count(*)" -> seq.size))
                        else b)
                  )
                }
              case Left(err) => err
            }
            .pipeTo(sender)
        case Success(ParsedSimpleQuery(_, _, _, true, _, fields, _)) if fields.lengthCompare(1) == 0 =>
          val distinctField = fields.head.name

          val filteredActors =
            actorsForLocations(locations)

          val shardResults = gatherAndGroupShardResults(filteredActors, statement, distinctField, schema) { values =>
            Bit(
              timestamp = 0,
              value = 0,
              dimensions = retrieveField(values, distinctField, (bit: Bit) => bit.dimensions),
              tags = retrieveField(values, distinctField, (bit: Bit) => bit.tags)
            )
          }

          applyOrderingWithLimit(shardResults, statement, schema)
            .map { generateResponse(statement.db, statement.namespace, statement.metric, _) }
            .pipeTo(sender)

        case Success(ParsedAggregatedQuery(_, _, _, InternalCountAggregation(_, _), _, _)) =>
          val filteredIndexes =
            actorsForLocations(locations)

          val shardResults = gatherAndGroupShardResults(filteredIndexes, statement, statement.groupBy.get, schema) {
            values =>
              Bit(0, values.map(_.value.asInstanceOf[Long]).sum, values.head.dimensions, values.head.tags)
          }

          applyOrderingWithLimit(shardResults, statement, schema)
            .map { generateResponse(statement.db, statement.namespace, statement.metric, _) }
            .pipeTo(sender)

        case Success(ParsedAggregatedQuery(_, _, _, aggregationType, _, _)) =>
          val filteredIndexes =
            actorsForLocations(locations)

          val rawResult = gatherAndGroupShardResults(filteredIndexes, statement, statement.groupBy.get, schema) {
            values =>
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

          applyOrderingWithLimit(rawResult, statement, schema)
            .map { resp =>
              generateResponse(statement.db, statement.namespace, statement.metric, resp)
            }
            .pipeTo(sender)

        case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
        case _           => sender ! SelectStatementFailed("Not a select statement.")
      }
    case DropMetric(_, _, metric) =>
      actorsForMetric(metric).foreach {
        case (key, actor) =>
          actor ! DeleteAll
          actors -= key
      }
    case Refresh(_, keys) =>
      keys.foreach { key =>
        getOrCreateActor(key) ! RefreshShard
      }
  }

  override def receive: Receive = readOps
}

object MetricReaderActor {

  def props(basePath: String, nodeName: String, db: String, namespace: String): Props =
    Props(new MetricReaderActor(basePath, nodeName, db, namespace))
}
