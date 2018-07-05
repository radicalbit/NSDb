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
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType}
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, Expression, SelectSQLStatement}
import io.radicalbit.nsdb.index.NumericType
import io.radicalbit.nsdb.index.lucene._
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.statement.{StatementParser, TimeRangeExtractor}
import spire.implicits._
import spire.math.Interval

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * Actor responsible for:
  *
  * - Retrieving data from shards, aggregates and returns it to the sender.
  *
  * @param basePath shards actors path.
  * @param db shards db.
  * @param namespace shards namespace.
  */
class MetricReaderActor(val basePath: String, val db: String, val namespace: String) extends Actor with ActorLogging {
  import scala.collection.mutable

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  private val actors: mutable.Map[ShardKey, ActorRef] = mutable.Map.empty

  /**
    * Gets or creates the actor for a given shard key.
    * @param key The shard key to identify the actor.
    * @return The existing or the created shard actor.
    */
  private def getOrCreateActor(key: ShardKey) = {
    actors.getOrElse(
      key, {
        val newActor = context.actorOf(ShardReaderActor.props(basePath, db, namespace, key),
                                       s"shard_reader_${key.metric}_${key.from}_${key.to}")
        actors += (key -> newActor)
        newActor
      }
    )
  }

  /**
    * Retrieve all the shard actors for a given metric.
    * @param metric The metric to filter shard actors
    * @return All the shard actors for a given metric.
    */
  private def actorsForMetric(metric: String) = actors.filter(_._1.metric == metric)

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
          val key        = ShardKey(metric, from.toLong, to.toLong)
          val shardActor = context.actorOf(ShardReaderActor.props(basePath, db, namespace, key))
          actors += (key -> shardActor)
      }
  }

  /**
    * Applies, if needed, ordering and limiting to results from multiple shards.
    * @param shardResult sequence of shard results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @return a single result obtained from the manipulation of multiple results from different shards.
    */
  private def applyOrderingWithLimit(shardResult: Future[Either[SelectStatementFailed, Seq[Bit]]],
                                     statement: SelectSQLStatement,
                                     schema: Schema): Future[Either[SelectStatementFailed, Seq[Bit]]] = {
    shardResult.map(s =>
      s.map { seq =>
        val maybeSorted = if (statement.order.isDefined) {
          val o = schema.fields.find(_.name == statement.order.get.dimension).get.indexType.ord
          implicit val ord: Ordering[JSerializable] =
            if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse
            else o
          seq.sortBy(_.fields(statement.order.get.dimension)._1)
        } else seq
        if (statement.limit.isDefined) maybeSorted.take(statement.limit.get.value) else maybeSorted
    })
  }

  private def filterShardsThroughTime[T](expression: Option[Expression], indexes: mutable.Map[ShardKey, T]) = {
    val intervals = TimeRangeExtractor.extractTimeRange(expression)
    indexes.filter {
      case (key, _) if intervals.nonEmpty =>
        intervals
          .map(i => Interval.closed(key.from, key.to).intersect(i) != Interval.empty[Long])
          .foldLeft(false)((x, y) => x || y)
      case _ => true
    }.toSeq
  }

  /**
    * Groups results coming from different shards according to the group by clause provided in the query.
    * @param statement the Sql statement to be executed againt every actor.
    * @param groupBy the group by clause dimension.
    * @param schema Metric's schema.
    * @param aggregationFunction the aggregate function corresponding to the aggregation operator (sum, count ecc.) contained in the query.
    * @return the grouped results.
    */
  private def gatherAndgroupShardResults(
      actors: Seq[(ShardKey, ActorRef)],
      statement: SelectSQLStatement,
      groupBy: String,
      schema: Schema)(aggregationFunction: Seq[Bit] => Bit): Future[Either[SelectStatementFailed, Seq[Bit]]] = {

    gatherShardResults(actors, statement, schema) { seq =>
      seq
        .groupBy(_.tags(groupBy))
        .mapValues(aggregationFunction)
        .values
        .toSeq
    }
  }

  /**
    * Gathers results from every shard actor and elaborate them.
    * @param actors Shard actors to retrieve results from.
    * @param statement the Sql statement to be executed againt every actor.
    * @param schema Metric's schema.
    * @param postProcFun The function that will be applied after data are retrieved from all the shards.
    * @return the processed results.
    */
  private def gatherShardResults(actors: Seq[(ShardKey, ActorRef)], statement: SelectSQLStatement, schema: Schema)(
      postProcFun: Seq[Bit] => Seq[Bit]): Future[Either[SelectStatementFailed, Seq[Bit]]] = {
    Future
      .sequence(actors.map {
        case (_, actor) => actor ? ExecuteSelectStatement(statement, schema)
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
    * @param statement raw statement.
    * @param parsedStatement parsed statement.
    * @param actors shard actors to retrieve data from.
    * @param schema metric's schema.
    * @return a single sequence of results obtained from different shards.
    */
  private def retrieveAndOrderPlainResults(statement: SelectSQLStatement,
                                           parsedStatement: ParsedSimpleQuery,
                                           actors: Seq[(ShardKey, ActorRef)],
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
          else
            seq.sortBy(_.tags(statement.order.get.dimension))

        sorted.take(statement.limit.get.value)
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

    case _ @ExecuteSelectStatement(statement, schema) =>
      StatementParser.parseStatement(statement, schema) match {
        case Success(parsedStatement @ ParsedSimpleQuery(_, _, _, false, limit, fields, _)) =>
          val actors =
            filterShardsThroughTime(statement.condition.map(_.expression), actorsForMetric(statement.metric))

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

          val filteredIndexes =
            filterShardsThroughTime(statement.condition.map(_.expression), actorsForMetric(statement.metric))

          val shardResults = gatherAndgroupShardResults(filteredIndexes, statement, distinctField, schema) { values =>
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

        case Success(ParsedAggregatedQuery(_, _, _, _: CountAllGroupsCollector[_], _, _)) =>
          val filteredIndexes =
            filterShardsThroughTime(statement.condition.map(_.expression), actorsForMetric(statement.metric))

          val shardResults = gatherAndgroupShardResults(filteredIndexes, statement, statement.groupBy.get, schema) {
            values =>
              Bit(0, values.map(_.value.asInstanceOf[Long]).sum, values.head.dimensions, values.head.tags)
          }

          applyOrderingWithLimit(shardResults, statement, schema)
            .map { generateResponse(statement.db, statement.namespace, statement.metric, _) }
            .pipeTo(sender)

        case Success(ParsedAggregatedQuery(_, _, _, collector, _, _)) =>
          val filteredIndexes =
            filterShardsThroughTime(statement.condition.map(_.expression), actorsForMetric(statement.metric))

          val rawResult =
            gatherAndgroupShardResults(filteredIndexes, statement, statement.groupBy.get, schema) { values =>
              val v                                        = schema.fields.find(_.name == "value").get.indexType.asInstanceOf[NumericType[_, _]]
              implicit val numeric: Numeric[JSerializable] = v.numeric
              collector match {
                case _: MaxAllGroupsCollector[_, _] =>
                  Bit(0, values.map(_.value).max, values.head.dimensions, values.head.tags)
                case _: MinAllGroupsCollector[_, _] =>
                  Bit(0, values.map(_.value).min, values.head.dimensions, values.head.tags)
                case _: SumAllGroupsCollector[_, _] =>
                  Bit(0, values.map(_.value).sum, values.head.dimensions, values.head.tags)
              }
            }

          applyOrderingWithLimit(rawResult, statement, schema)
            .map { generateResponse(statement.db, statement.namespace, statement.metric, _) }
            .pipeTo(sender)

        case Failure(ex) => Failure(ex)
        case _           => Failure(new InvalidStatementException("Not a select statement."))
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
                            extract: (Bit) => Map[String, JSerializable]): Map[String, JSerializable] =
    values.headOption
      .flatMap(bit => extract(bit).get(field).map(x => Map(field -> x)))
      .getOrElse(Map.empty[String, JSerializable])

  /**
    * This is a utility method in charge to associate a dimension or a tag with the given count.
    * It extracts the field from a Bit sequence in a functional way without having the risk to throw dangerous exceptions.
    *
    * @param values the sequence of bits holding the field to be extracted.
    * @param count the value of the count to be associated with the field.
    * @param extract the function defining how to extract the field from a given bit.
    * @return
    */
  private def retrieveCount(values: Seq[Bit],
                            count: Int,
                            extract: (Bit) => Map[String, JSerializable]): Map[String, JSerializable] =
    values.headOption
      .flatMap(bit => extract(bit).headOption.map(x => Map(x._1 -> count.asInstanceOf[JSerializable])))
      .getOrElse(Map.empty[String, JSerializable])

}

object MetricReaderActor {

  def props(basePath: String, db: String, namespace: String): Props =
    Props(new MetricReaderActor(basePath, db, namespace))
}
