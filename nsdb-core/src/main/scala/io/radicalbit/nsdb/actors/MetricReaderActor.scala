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

package io.radicalbit.nsdb.actors

import java.math.{MathContext, RoundingMode}
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props, Terminated}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.ShardReaderActor.RefreshShard
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel.{globalTimeout, precision}
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType, ValueFieldType}
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, SelectSQLStatement}
import io.radicalbit.nsdb.common.{NSDbLongType, NSDbType}
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.post_proc._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.util.ErrorManagementUtils

import scala.concurrent.{ExecutionContextExecutor, Future}

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
    Timeout(context.system.settings.config.getDuration(globalTimeout, TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val mathContext: MathContext =
    new MathContext(context.system.settings.config.getInt(precision), RoundingMode.HALF_UP)

  private val actors: mutable.Map[Location, ActorRef] = mutable.Map.empty

  /**
    * Gets or creates the actor for a given shard key.
    *
    * @param location The shard key to identify the actor.
    * @return The existing or the created shard actor.
    */
  private def getOrCreateShardReaderActor(location: Location): ActorRef =
    actors.getOrElse(
      location, {
        val shardActor = context.actorOf(ShardReaderActor.props(basePath, db, namespace, location), actorName(location))
        context.watch(shardActor)
        actors += (location -> shardActor)
        shardActor
      }
    )

  /**
    * Gets if exists the actor for a given shard key.
    *
    * @param location The shard key to identify the actor.
    * @return The existing shard actor, None if it does not exist.
    */
  private def getShardReaderActor(location: Location): Option[ActorRef] = actors.get(location)

  private def actorName(location: Location) =
    s"shard_reader-${location.node}-${location.metric}-${location.from}-${location.to}"

  private def location(actorName: String): Option[Location] =
    actorName.split("-").takeRight(4) match {
      case Array(metric, node, from, to) => Some(Location(node, metric, from.toLong, to.toLong))
      case _                             => None
    }

  /**
    * Retrieve all the shard actors of a metrics given a set of locations.
    *
    * @param locations locations to filter the shard actors with.
    * @return filtered map containing all the actors for the given locations.
    */
  private def actorsForLocations(locations: Seq[Location]): Seq[(Location, ActorRef)] =
    locations.map(location => (location, getOrCreateShardReaderActor(location)))

  /**
    * Any existing shard is retrieved
    */
  override def preStart: Unit = {
    Option(Paths.get(basePath, db, namespace, "shards").toFile.list())
      .map(_.toSet)
      .getOrElse(Set.empty)
      .foreach {
        case shardName if shardName.split("_").length == 3 =>
          val Array(metric, from, to) = shardName.split("_")
          val location                = Location(metric, nodeName, from.toLong, to.toLong)
          val shardActor =
            context.actorOf(ShardReaderActor.props(basePath, db, namespace, location), actorName(location))
          context.watch(shardActor)
          actors += (location -> shardActor)
        case _ => //do nothing
      }
  }

  /**
    * Groups results coming from different shards according to the group by clause provided in the query.
    *
    * @param statement the select sql statement.
    * @param actors Shard actors to retrieve results from.
    * @param groupBy the group by clause dimension.
    * @param msg the original [[ExecuteSelectStatement]] command
    * @param aggregationFunction the aggregate function corresponding to the aggregation operator (sum, count ecc.) contained in the query.
    * @return the grouped results.
    */
  private def gatherAndGroupShardResults(
      statement: SelectSQLStatement,
      actors: Seq[(Location, ActorRef)],
      groupBy: String,
      msg: ExecuteSelectStatement)(aggregationFunction: Seq[Bit] => Bit): Future[ExecuteSelectStatementResponse] = {

    gatherShardResults(statement, actors, msg) { bits =>
      bits
        .groupBy(_.tags(groupBy))
        .mapValues(aggregationFunction)
        .values
        .toSeq
    }
  }

  /**
    * Gathers results from every shard actor and elaborate them.
    *
    * @param statement the select sql statement.
    * @param actors Shard actors to retrieve results from.
    * @param msg the original [[ExecuteSelectStatement]] command
    * @param postProcFun The function that will be applied after data are retrieved from all the shards.
    * @return the processed results.
    */
  private def gatherShardResults(
      statement: SelectSQLStatement,
      actors: Seq[(Location, ActorRef)],
      msg: ExecuteSelectStatement)(postProcFun: Seq[Bit] => Seq[Bit]): Future[ExecuteSelectStatementResponse] = {

    /**
      * Retrieve each shard actor bits result at a time checking condition at each iteration
      * function called when time ordering and limit condition are present
      * @param index for iterate the shard actors
      * @param previousFuture future containing the incremental previous results
      */
    def iterativeShardActorsResult(index: Int, previousFuture: Future[Seq[Any]]): Future[Seq[Any]] = {
      previousFuture.flatMap { previousResults =>
        val parsedPreviousResults = previousResults.asInstanceOf[Seq[SelectStatementExecuted]].flatMap(_.values)
        if ((index < actors.size) && parsedPreviousResults.size < statement.limit.get.value) {
          val currentFuture = (actors(index)._2 ? msg.copy(locations = actors.map(_._1))).recoverWith {
            case t => Future(SelectStatementFailed(statement, t.getMessage))
          }
          iterativeShardActorsResult(index + 1, currentFuture.map(previousResults :+ _))
        } else previousFuture
      }
    }

    (if ((statement.getTimeOrdering.isDefined || statement.order.isEmpty) && statement.limit.isDefined)
       iterativeShardActorsResult(0, Future(Seq.empty[Any]))
     else {
       Future
         .sequence(actors.map {
           case (_, actor) =>
             (actor ? msg.copy(locations = actors.map(_._1)))
               .recoverWith { case t => Future(SelectStatementFailed(statement, t.getMessage)) }
         })
     })
      .map(ErrorManagementUtils.partitionResponses[SelectStatementExecuted, SelectStatementFailed])
      .map {
        case (successes, Nil) =>
          SelectStatementExecuted(statement, postProcFun(successes.flatMap(_.values)), msg.schema)
        case (_, errors) => SelectStatementFailed(statement, errors.map(_.reason).mkString(","))
      }
  }

  /**
    * Retrieves and order results from different shards in case the statement does not contains aggregations
    * and a where condition involving timestamp has been provided.
    *
    * @param actors shard actors to retrieve data from.
    * @param parsedStatement parsed statement.
    * @param msg the original [[ExecuteSelectStatement]] command
    * @return a single sequence of results obtained from different shards.
    */
  private def retrieveAndOrderPlainResults(actors: Seq[(Location, ActorRef)],
                                           parsedStatement: ParsedSimpleQuery,
                                           msg: ExecuteSelectStatement): Future[ExecuteSelectStatementResponse] = {

    val statement = msg.selectStatement
    if (statement.getTimeOrdering.isDefined || statement.order.isEmpty) {

      val eventuallyOrderedActors =
        statement.getTimeOrdering.map(actors.sortBy(_._1.from)(_)).getOrElse(actors)

      gatherShardResults(statement, eventuallyOrderedActors, msg) { seq =>
        seq.take(parsedStatement.limit)
      }

    } else {

      gatherShardResults(statement, actors, msg) { seq =>
        val schemaField = msg.schema.fieldsMap(statement.order.get.dimension)
        val o           = schemaField.indexType.ord
        implicit val ord: Ordering[Any] =
          if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse else o

        val sorted =
          if (schemaField.fieldClassType == DimensionFieldType)
            seq.sortBy(_.dimensions(statement.order.get.dimension).rawValue)
          else if (schemaField.fieldClassType == ValueFieldType)
            seq.sortBy(_.value.rawValue)
          else
            seq.sortBy(_.tags(statement.order.get.dimension).rawValue)

        statement.limit.map(l => sorted.take(l.value)).getOrElse(sorted)
      }
    }
  }

  /**
    * behaviour for read operations.
    *
    * - [[Terminated]] received when a child has been stopped
    *
    * - [[ExecuteSelectStatement]] execute a given sql statement.
    */
  def readOps: Receive = {
    case Terminated(actor) =>
      location(actor.path.name).foreach { location =>
        log.debug("removing not used actor for location", location)
        actors -= location
      }
    case msg @ GetCountWithLocations(_, ns, metric, locations) =>
      Future
        .sequence(actorsForLocations(locations).map {
          case (_, actor) =>
            (actor ? msg).mapTo[CountGot].map(_.count)
        })
        .map(s => CountGot(db, ns, metric, s.sum))
        .pipeTo(sender)
    case msg @ ExecuteSelectStatement(statement, schema, locations, _, timeContext, isSingleNode) =>
      log.debug("executing statement in metric reader actor {}", statement)
      StatementParser.parseStatement(statement, schema)(timeContext) match {
        case Right(parsedStatement @ ParsedSimpleQuery(_, _, _, false, _, _, _)) =>
          retrieveAndOrderPlainResults(actorsForLocations(locations), parsedStatement, msg)
            .map {
              case SelectStatementExecuted(_, seq, schema) =>
                SelectStatementExecuted(
                  statement,
                  seq,
                  schema
                )
              case err: SelectStatementFailed => err
            }
            .pipeTo(sender)
        case Right(ParsedSimpleQuery(_, _, _, true, _, fields, _)) if fields.lengthCompare(1) == 0 =>
          val distinctField = fields.head.name

          val filteredActors =
            actorsForLocations(locations)

          val shardResults = gatherAndGroupShardResults(statement, filteredActors, distinctField, msg) { values =>
            Bit(
              timestamp = 0,
              value = NSDbLongType(0),
              dimensions = retrieveField(values, distinctField, (bit: Bit) => bit.dimensions),
              tags = retrieveField(values, distinctField, (bit: Bit) => bit.tags)
            )
          }

          shardResults.pipeTo(sender)

        case Right(ParsedGlobalAggregatedQuery(_, _, _, _, fields, aggregations, _)) =>
          gatherShardResults(statement, actorsForLocations(locations), msg) { rawResults =>
            globalAggregationReduce(rawResults, fields, aggregations, statement, schema, isSingleNode)
          } pipeTo sender

        case Right(ParsedAggregatedQuery(_, _, _, aggregation, _, _)) =>
          val filteredIndexes =
            actorsForLocations(locations)

          val shardResults =
            gatherAndGroupShardResults(statement, filteredIndexes, statement.groupBy.get.field, msg)(
              internalAggregationReduce(_, schema, aggregation, isSingleNode))

          shardResults.pipeTo(sender)
        case Right(ParsedTemporalAggregatedQuery(_, _, _, _, aggregationType, _, _, _, _)) =>
          val actors =
            actorsForLocations(locations)

          gatherShardResults(statement, actors, msg)(
            postProcessingTemporalQueryResult(schema, statement, aggregationType, isSingleNode)).pipeTo(sender)

        case Left(error) => sender ! SelectStatementFailed(statement, error)
      }
    case DeleteAllMetrics(_, _) =>
      actors.foreach {
        case (loc, actor) =>
          actor ! PoisonPill
          actors -= loc
      }
    case DropMetricWithLocations(_, _, _, locations) =>
      actors.foreach {
        case (loc, actor) if locations.contains(loc) =>
          actor ! PoisonPill
          actors -= loc
        case _ => //do nothing
      }
    case EvictShard(_, _, location) =>
      actors.get(location).foreach { actor =>
        actor ! PoisonPill
        actors -= location
      }
    case Refresh(_, locations) =>
      log.debug(s"refreshing locations $locations")
      locations.foreach { location =>
        getShardReaderActor(location).foreach(_ ! RefreshShard)
      }
  }

  override def receive: Receive = readOps

  /**
    * This is a utility method to extract dimensions or tags from a Bit sequence in a functional way without having
    * the risk to throw dangerous exceptions.
    *
    * @param values the sequence of bits holding the fields to be extracted.
    * @param field the name of the field to be extracted.
    * @param extract the function defining how to extract the field from a given bit.
    * @return
    */
  private def retrieveField(values: Seq[Bit],
                            field: String,
                            extract: Bit => Map[String, NSDbType]): Map[String, NSDbType] =
    values.headOption
      .flatMap(bit => extract(bit).get(field).map(x => Map(field -> x)))
      .getOrElse(Map.empty[String, NSDbType])
}

object MetricReaderActor {

  def props(basePath: String, nodeName: String, db: String, namespace: String): Props =
    Props(new MetricReaderActor(basePath, nodeName, db, namespace))
}
