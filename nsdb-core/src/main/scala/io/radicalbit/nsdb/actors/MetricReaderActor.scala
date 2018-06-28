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
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, Expression, SelectSQLStatement}
import io.radicalbit.nsdb.index.NumericType
import io.radicalbit.nsdb.index.lucene._
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.statement.{StatementParser, TimeRangeExtractor}
import org.apache.lucene.index.IndexNotFoundException
import spire.implicits._
import spire.math.Interval

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
  * Actor responsible for:
  *
  * - Retrieving data from shards, aggregates and returns it to the sender.
  *
  * @param basePath shards indexes path.
  * @param db shards db.
  * @param namespace shards namespace.
  */
class MetricReaderActor(val basePath: String, val db: String, val namespace: String)
    extends Actor
    //    with MetricsActor
    with ActorLogging {
  import scala.collection.mutable

  private val statementParser = new StatementParser()

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  //  override def getIndex(key: ShardKey): TimeSeriesIndex =
  //    shards.getOrElse(
  //      key, {
  //        val directory =
  //          new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}"))
  //        val newIndex = new TimeSeriesIndex(directory)
  //        shards += (key -> newIndex)
  //        newIndex
  //      }
  //    )

  val actors: mutable.Map[ShardKey, ActorRef] = mutable.Map.empty

  def getActor(key: ShardKey) = {
    actors.get(key)
  }

  def getOrCreateActor(key: ShardKey) = {
    actors.getOrElse(key, {
      val newActor = context.actorOf(ShardReaderActor.props(basePath, db, namespace, key))
      actors += (key -> newActor)
      newActor
    })
  }

  protected def actorsForMetric(metric: String) = actors.filter(_._1.metric == metric)

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
        //          val directory =
        //            new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_$to"))
        //          val newIndex = new TimeSeriesIndex(directory)
        //          shards += (ShardKey(metric, from.toLong, to.toLong) -> newIndex)
        //          val directoryFacets =
        //            new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_$to", "facet"))
        //          val taxoDirectoryFacets =
        //            new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_$to", "facet", "taxo"))
        //          val newFacetIndex = new FacetIndex(directoryFacets, taxoDirectoryFacets)
        //          facetIndexShards += (ShardKey(metric, from.toLong, to.toLong) -> newFacetIndex)
      }
  }

  private def handleQueryResults(metric: String, out: Try[Seq[Bit]]) = {
    out.recoverWith {
      case _: IndexNotFoundException => Success(Seq.empty)
    }
  }

  /**
    * Applies, if needed, ordering and limiting to results from multiple shards.
    * @param shardResult sequence of shard results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @return a single result obtained from the manipulation of multiple results from different shards.
    */
  private def applyOrderingWithLimit(shardResult: Future[Seq[Bit]], statement: SelectSQLStatement, schema: Schema) = {
    shardResult.map(s => {
      val maybeSorted = if (statement.order.isDefined) {
        val o = schema.fields.find(_.name == statement.order.get.dimension).get.indexType.ord
        implicit val ord: Ordering[JSerializable] =
          if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse
          else o
        s.sortBy(_.fields(statement.order.get.dimension))
      } else s

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
    * @param shardResults results coming from different shards.
    * @param dimension the group by clause dimension
    * @param aggregationFunction the aggregate function corresponding to the aggregation operator (sum, count ecc.) contained in the query.
    * @return the grouped results.
    */
  private def groupShardResults[W](shardResults: Future[Seq[Bit]], dimension: String)(
      aggregationFunction: Seq[Bit] => W) = {
    shardResults.map(
      results =>
        results
          .groupBy(_.dimensions(dimension))
          .mapValues(aggregationFunction)
          .values
          .toSeq)
  }

  /**
    * Retrieves and order results from different shards in case the statement does not contains aggregations
    * and a where condition involving timestamp has been provided.
    * @param statement raw statement.
    * @param parsedStatement parsed statement.
    * @param indexes shard indexes to retrieve data from.
    * @param schema metric's schema.
    * @return a single sequence of results obtained from different shards.
    */
  private def retrieveAndorderPlainResults(statement: SelectSQLStatement,
                                           parsedStatement: ParsedSimpleQuery,
                                           indexes: Seq[(ShardKey, ActorRef)],
                                           schema: Schema): Future[Seq[Bit]] = {
    //    val (_, metric, q, _, limit, fields, sort) = ParsedSimpleQuery.unapply(parsedStatement).get
    if (statement.getTimeOrdering.isDefined || statement.order.isEmpty) {
      //      val result: ListBuffer[Seq[Bit]] = ListBuffer.empty

      val eventuallyOrderedActors =
        statement.getTimeOrdering.map(indexes.sortBy(_._1.from)(_)).getOrElse(indexes)

      //      eventuallyOrdered.takeWhile {
      //        case (_, index) =>
      //          val partials = handleQueryResults(metric, Try(index.query(q, fields, limit, sort)(identity)))
      //          result += partials
      //
      //          val combined = Try(result.flatMap(_.get))
      //
      //          combined.isSuccess && combined.get.lengthCompare(statement.limit.map(_.value).getOrElse(Int.MaxValue)) < 0
      //      }
      //
      //      Try(result.flatMap(_.get))

      Future
        .sequence(eventuallyOrderedActors.map {
          case (_, actor) => (actor ? ExecuteSelectStatement(statement, schema)).mapTo[SelectStatementExecuted]
        })
        .map(
          e => e.flatMap(_.values).take(parsedStatement.limit)
        )

    } else {

      Future
        .sequence(indexes.map {
          case (_, actor) =>
            //          handleQueryResults(metric, Try(index.query(q, fields, limit, sort)(identity)))
            (actor ? ExecuteSelectStatement(statement, schema)).mapTo[SelectStatementExecuted]
        })
        .map(
          e => e.flatMap(_.values)
        )
        .map(s => {
          val o = schema.fields.find(_.name == statement.order.get.dimension).get.indexType.ord
          implicit val ord: Ordering[JSerializable] =
            if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse else o
          val sorted = s.sortBy(_.dimensions(statement.order.get.dimension))
          sorted.take(statement.limit.get.value)
        })

    }
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
      //      Future.sequence()
      Future
        .sequence(actorsForMetric(metric).map {
          case (_, actor) =>
            (actor ? msg).mapTo[CountGot].map(_.count)
          //          index.query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None)(identity).size
        })
        .map(_.sum)
        .map(s => CountGot(db, ns, metric, s))
        .pipeTo(sender)

    case msg @ ExecuteSelectStatement(statement, schema) =>
      //      val postProcessedResult =
      statementParser.parseStatement(statement, schema) match {
        case Success(parsedStatement @ ParsedSimpleQuery(_, _, _, false, limit, fields, _)) =>
          val actors =
            filterShardsThroughTime(statement.condition.map(_.expression), actorsForMetric(statement.metric))

          val orderedResults = retrieveAndorderPlainResults(statement, parsedStatement, actors, schema)

          val f = if (fields.lengthCompare(1) == 0 && fields.head.count) {
            orderedResults.map(seq => {
              val recordCount = seq.map(_.value.asInstanceOf[Int]).sum
              val count       = if (recordCount <= limit) recordCount else limit
              SelectStatementExecuted(statement.db,
                                      statement.namespace,
                                      statement.metric,
                                      Seq(Bit(0, count, Map(seq.head.dimensions.head._1 -> count), Map.empty)))
            })
          } else
            orderedResults.map(
              s => {
                val values = s.map(
                  b =>
                    if (b.dimensions.contains("count(*)")) b.copy(dimensions = b.dimensions + ("count(*)" -> s.size))
                    else b)
                SelectStatementExecuted(statement.db, statement.namespace, statement.metric, values)
              }
            )
          f.pipeTo(sender)

        case Success(ParsedSimpleQuery(_, metric, q, true, limit, fields, sort)) if fields.lengthCompare(1) == 0 =>
          val distinctField = fields.head.name

          val filteredIndexes =
            filterShardsThroughTime(statement.condition.map(_.expression), actorsForMetric(statement.metric))

          val results = Future
            .sequence(filteredIndexes.map { case (_, actor) => (actor ? msg).mapTo[SelectStatementExecuted] })
            .map(
              e => e.flatMap(_.values)
            )
          //            val results = filteredIndexes.map {
          //              case (_, index) =>
          //                handleQueryResults(metric, Try(index.getDistinctField(q, fields.map(_.name).head, sort, limit)))
          //            }

          val shardResults = groupShardResults(results, distinctField) { values =>
            Bit(0, 0, Map[String, JSerializable]((distinctField, values.head.dimensions(distinctField))), Map.empty)
          }

          applyOrderingWithLimit(shardResults, statement, schema)
            .map(v => SelectStatementExecuted(statement.db, statement.namespace, statement.metric, v))
            .pipeTo(sender)

        case Success(ParsedAggregatedQuery(_, metric, q, collector: CountAllGroupsCollector[_], sort, limit)) =>
          val filteredIndexes =
            filterShardsThroughTime(statement.condition.map(_.expression), actorsForMetric(statement.metric))

          //              .map {
          //              case (_, index) =>
          //                handleQueryResults(
          //                  metric,
          //                  Try(index
          //                    .getCount(q, collector.groupField, sort, limit, schema.fieldsMap(collector.groupField).indexType)))
          //            }

          val result = Future
            .sequence(filteredIndexes.map { case (_, actor) => (actor ? msg).mapTo[SelectStatementExecuted] })
            .map(
              e => e.flatMap(_.values)
            )

          val shardResults = groupShardResults(result, statement.groupBy.get) { values =>
            Bit(0, values.map(_.value.asInstanceOf[Long]).sum, values.head.dimensions, Map.empty)
          }

          applyOrderingWithLimit(shardResults, statement, schema)
            .map(values => SelectStatementExecuted(statement.db, statement.namespace, statement.metric, values))
            .pipeTo(sender)

        case Success(ParsedAggregatedQuery(_, metric, q, collector, sort, limit)) =>
          val shardResults = Future
            .sequence(actorsForMetric(statement.metric).toSeq.map {
              case (_, actor) =>
                //                handleQueryResults(metric, Try(index.query(q, collector.clear, limit, sort)))
                (actor ? msg).mapTo[SelectStatementExecuted]
            })
            .map(
              e => e.flatMap(_.values)
            )
          val rawResult =
            groupShardResults(shardResults, statement.groupBy.get) { values =>
              val v                                        = schema.fields.find(_.name == "value").get.indexType.asInstanceOf[NumericType[_, _]]
              implicit val numeric: Numeric[JSerializable] = v.numeric
              collector match {
                case _: MaxAllGroupsCollector[_, _] =>
                  Bit(0, values.map(_.value).max, values.head.dimensions, Map.empty)
                case _: MinAllGroupsCollector[_, _] =>
                  Bit(0, values.map(_.value).min, values.head.dimensions, Map.empty)
                case _: SumAllGroupsCollector[_, _] =>
                  Bit(0, values.map(_.value).sum, values.head.dimensions, Map.empty)
              }
            }

          applyOrderingWithLimit(rawResult, statement, schema)
            .map(values => SelectStatementExecuted(statement.db, statement.namespace, statement.metric, values))
            .pipeTo(sender)

        case Failure(ex) => Failure(ex)
        case _           => Failure(new InvalidStatementException("Not a select statement."))
      }

    //      postProcessedResult match {
    //        case Success(bits)                          => sender() ! SelectStatementExecuted(db, namespace, statement.metric, bits)
    //        case Failure(ex: InvalidStatementException) => sender() ! SelectStatementFailed(ex.message)
    //        case Failure(ex)                            => sender() ! SelectStatementFailed(ex.getMessage)
    //      }
    case DropMetric(_, _, metric) =>
      actorsForMetric(metric).foreach {
        case (key, actor) =>
          //          implicit val writer: IndexWriter = index.getWriter
          //          index.deleteAll()
          //          writer.close()
          //          index.refresh()
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

  def props(basePath: String, db: String, namespace: String): Props =
    Props(new MetricReaderActor(basePath, db, namespace))
}
