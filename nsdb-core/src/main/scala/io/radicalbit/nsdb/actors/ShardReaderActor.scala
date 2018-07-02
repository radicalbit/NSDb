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
import akka.util.Timeout
import io.radicalbit.nsdb.actors.ShardAccumulatorActor.Refresh
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType}
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, Expression, SelectSQLStatement}
import io.radicalbit.nsdb.index.lucene._
import io.radicalbit.nsdb.index.{FacetIndex, NumericType, TimeSeriesIndex}
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.statement.{StatementParser, TimeRangeExtractor}
import org.apache.lucene.index.{IndexNotFoundException, IndexWriter}
import org.apache.lucene.search.{MatchAllDocsQuery, Query, Sort}
import org.apache.lucene.store.MMapDirectory
import spire.implicits._
import spire.math.Interval

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Actor responsible for:
  *
  * - Accumulating write and delete operations which will be performed by [[ShardPerformerActor]].
  *
  * - Retrieving data from shards, aggregates and returns it to the sender.
  *
  * @param basePath shards indexes path.
  * @param db shards db.
  * @param namespace shards namespace.
  */
class ShardReaderActor(val basePath: String, val db: String, val namespace: String)
    extends Actor
    with ShardsActor
    with ActorLogging {
  import scala.collection.mutable

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  /**
    * Actor responsible for the actual writes into indexes.
    */
  var performerActor: ActorRef = _

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  /**
    * Writes scheduler interval.
    */
  lazy val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  override def getIndex(key: ShardKey) =
    shards.getOrElse(
      key, {
        val directory =
          new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}"))
        val newIndex = new TimeSeriesIndex(directory)
        shards += (key -> newIndex)
        newIndex
      }
    )

  private def handleQueryResults(metric: String, out: Try[Seq[Bit]]): Try[Seq[Bit]] = {
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
  private def applyOrderingWithLimit(shardResult: Try[Seq[Bit]], statement: SelectSQLStatement, schema: Schema) = {
    Try(shardResult.get).map(s => {
      val maybeSorted = if (statement.order.isDefined) {
        val o = schema.fields.find(_.name == statement.order.get.dimension).get.indexType.ord
        implicit val ord: Ordering[JSerializable] =
          if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse
          else o
        s.sortBy(_.fields(statement.order.get.dimension)._1)
      } else s

      if (statement.limit.isDefined) maybeSorted.take(statement.limit.get.value) else maybeSorted
    })
  }

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
          val directory =
            new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_$to"))
          val newIndex = new TimeSeriesIndex(directory)
          shards += (ShardKey(metric, from.toLong, to.toLong) -> newIndex)
          val directoryFacets =
            new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_$to", "facet"))
          val taxoDirectoryFacets =
            new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${metric}_${from}_$to", "facet", "taxo"))
          val newFacetIndex = new FacetIndex(directoryFacets, taxoDirectoryFacets)
          facetIndexShards += (ShardKey(metric, from.toLong, to.toLong) -> newFacetIndex)
      }
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
    * @param tag the group by clause tag
    * @param aggregationFunction the aggregate function corresponding to the aggregation operator (sum, count ecc.) contained in the query.
    * @return the grouped results.
    */
  private def groupShardResults[W](shardResults: Seq[Try[Seq[Bit]]], tag: String)(
      aggregationFunction: Seq[Bit] => W): Try[Seq[W]] = {
    Try(
      shardResults
        .flatMap(_.get)
        .groupBy(_.tags(tag))
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
  private def retrieveAndOrderPlainResults(statement: SelectSQLStatement,
                                           parsedStatement: ParsedSimpleQuery,
                                           indexes: Seq[(ShardKey, TimeSeriesIndex)],
                                           schema: Schema): Try[Seq[Bit]] = {
    val (_, metric, q, _, limit, fields, sort) = ParsedSimpleQuery.unapply(parsedStatement).get
    if (statement.getTimeOrdering.isDefined || statement.order.isEmpty) {
      val result: ListBuffer[Try[Seq[Bit]]] = ListBuffer.empty

      val eventuallyOrdered =
        statement.getTimeOrdering.map(indexes.sortBy(_._1.from)(_)).getOrElse(indexes)

      eventuallyOrdered.takeWhile {
        case (_, index) =>
          val partials = handleQueryResults(metric, Try(index.query(schema, q, fields, limit, sort)(identity)))
          result += partials

          val combined = Try(result.flatMap(_.get))

          combined.isSuccess && combined.get.lengthCompare(statement.limit.map(_.value).getOrElse(Int.MaxValue)) < 0
      }

      Try(result.flatMap(_.get))

    } else {

      val shardResults = indexes.map {
        case (_, index) =>
          handleQueryResults(metric, Try(index.query(schema, q, fields, limit, sort)(identity)))
      }

      Try(shardResults.flatMap(_.get)).map(s => {
        val schemaField = schema.fields.find(_.name == statement.order.get.dimension).get
        val o           = schemaField.indexType.ord
        implicit val ord: Ordering[JSerializable] =
          if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse else o

        val sorted =
          if (schemaField.fieldClassType == DimensionFieldType)
            s.sortBy(_.dimensions(statement.order.get.dimension))
          else
            s.sortBy(_.tags(statement.order.get.dimension))

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
      sender() ! MetricsGot(db, namespace, shards.keys.map(_.metric).toSet)
    case GetCount(_, ns, metric) =>
      val hits = shardsForMetric(metric).map {
        case (_, index) =>
          index.getCount()
      }.sum
      sender ! CountGot(db, ns, metric, hits)
    case ExecuteSelectStatement(statement, schema) =>
      executeSelectStatement(statement, schema)
    case DropMetric(_, _, metric) =>
      shardsForMetric(metric).foreach {
        case (key, index) =>
          implicit val writer: IndexWriter = index.getWriter
          index.deleteAll()
          writer.close()
          index.refresh()
          shards -= key
      }
    case Refresh(_, keys) =>
      keys.foreach { key =>
        getIndex(key).refresh()
        getFacetIndex(key).refresh()
      }
  }

  private def executeSelectStatement(statement: SelectSQLStatement, schema: Schema): Unit = {

    val parsedStatement = StatementParser.parseStatement(statement, schema)

    val postProcessedResult: Try[Seq[Bit]] = parsedStatement match {
      case Success(parsedStatement @ ParsedSimpleQuery(_, _, _, false, limit, fields, _)) =>
        executeSimpleQuery(statement, schema, parsedStatement, limit, fields)

      case Success(ParsedSimpleQuery(_, metric, q, true, limit, fields, sort)) if fields.lengthCompare(1) == 0 =>
        executeSimpleQueryFixedLength(statement, schema, metric, q, limit, fields, sort)

      case Success(ParsedAggregatedQuery(_, metric, q, collector: CountAllGroupsCollector[_], sort, limit)) =>
        executeAggregateQueryWithCount(statement, schema, metric, q, collector, sort, limit)

      case Success(ParsedAggregatedQuery(_, metric, q, collector, sort, limit)) =>
        executeAggregationQuery(statement, schema, metric, q, collector, sort, limit)

      case Failure(ex) => Failure(ex)
      case _ =>
        log.error("An error occurred trying to execute the following select statement {}.", statement)
        Failure(new InvalidStatementException("Not a select statement."))
    }

    postProcessedResult match {
      case Success(bits) =>
        sender() ! SelectStatementExecuted(db, namespace, statement.metric, bits)
      case Failure(ex: InvalidStatementException) => sender() ! SelectStatementFailed(ex.message)
      case Failure(ex)                            => sender() ! SelectStatementFailed(ex.getMessage)
    }
  }

  private def executeSimpleQuery(statement: SelectSQLStatement,
                                 schema: Schema,
                                 parsedStatement: ParsedSimpleQuery,
                                 limit: Int,
                                 fields: List[SimpleField]): Try[Seq[Bit]] = {
    val indexes =
      filterShardsThroughTime(statement.condition.map(_.expression), shardsForMetric(statement.metric))

    val orderedResults = retrieveAndOrderPlainResults(statement, parsedStatement, indexes, schema)

    if (fields.lengthCompare(1) == 0 && fields.head.count) {
      orderedResults.map(seq => {
        val recordCount = seq.map(_.value.asInstanceOf[Int]).sum
        val count       = if (recordCount <= limit) recordCount else limit

        Seq(
          Bit(timestamp = 0,
              value = count,
              dimensions = retrieveCount(seq, count, (bit: Bit) => bit.dimensions),
              tags = retrieveCount(seq, count, (bit: Bit) => bit.tags)))
      })
    } else
      orderedResults.map(
        s =>
          s.map(
            b =>
              if (b.tags.contains("count(*)")) b.copy(tags = b.tags + ("count(*)" -> s.size))
              else b)
      )
  }

  private def executeSimpleQueryFixedLength(statement: SelectSQLStatement,
                                            schema: Schema,
                                            metric: String,
                                            q: Query,
                                            limit: Int,
                                            fields: List[SimpleField],
                                            sort: Option[Sort]): Try[Seq[Bit]] = {
    val distinctField = fields.head.name

    val filteredIndexes =
      filterShardsThroughTime(statement.condition.map(_.expression), facetsShardsFromMetric(statement.metric))

    val results = filteredIndexes.map {
      case (_, index) =>
        handleQueryResults(metric, Try(index.getDistinctField(q, fields.map(_.name).head, sort, limit)))
    }

    val shardResults = groupShardResults(results, distinctField) { values =>
      Bit(
        timestamp = 0,
        value = 0,
        dimensions = retrieveField(values, distinctField, (bit: Bit) => bit.dimensions),
        tags = retrieveField(values, distinctField, (bit: Bit) => bit.tags)
      )
    }

    applyOrderingWithLimit(shardResults, statement, schema)
  }

  private def executeAggregateQueryWithCount(statement: SelectSQLStatement,
                                             schema: Schema,
                                             metric: String,
                                             q: Query,
                                             collector: CountAllGroupsCollector[_],
                                             sort: Option[Sort],
                                             limit: Option[Int]): Try[Seq[Bit]] = {
    val result =
      filterShardsThroughTime(statement.condition.map(_.expression), facetsShardsFromMetric(statement.metric)).map {
        case (_, index) =>
          handleQueryResults(
            metric,
            Try(
              index
                .getCount(q, collector.groupField, sort, limit, schema.fieldsMap(collector.groupField).indexType)))
      }

    val shardResults = groupShardResults(result, statement.groupBy.get) { values =>
      Bit(timestamp = 0,
          value = values.map(_.value.asInstanceOf[Long]).sum,
          dimensions = values.head.dimensions,
          tags = values.head.tags)
    }

    applyOrderingWithLimit(shardResults, statement, schema)
  }

  private def executeAggregationQuery(statement: SelectSQLStatement,
                                      schema: Schema,
                                      metric: String,
                                      q: Query,
                                      collector: AllGroupsAggregationCollector[_, _],
                                      sort: Option[Sort],
                                      limit: Option[Int]): Try[Seq[Bit]] = {
    val shardResults = shardsForMetric(statement.metric).toSeq.map {
      case (_, index) =>
        handleQueryResults(metric, Try(index.query(schema, q, collector.clear, limit, sort)))
    }
    val rawResult =
      groupShardResults(shardResults, statement.groupBy.get) { values =>
        val v                                        = schema.fields.find(_.name == "value").get.indexType.asInstanceOf[NumericType[_, _]]
        implicit val numeric: Numeric[JSerializable] = v.numeric
        collector match {
          case _: MaxAllGroupsCollector[_, _] =>
            Bit(timestamp = 0,
                value = values.map(_.value).max,
                dimensions = values.head.dimensions,
                tags = values.head.tags)
          case _: MinAllGroupsCollector[_, _] =>
            Bit(timestamp = 0,
                value = values.map(_.value).min,
                dimensions = values.head.dimensions,
                tags = values.head.tags)
          case _: SumAllGroupsCollector[_, _] =>
            Bit(timestamp = 0,
                value = values.map(_.value).sum,
                dimensions = values.head.dimensions,
                tags = values.head.tags)
        }
      }

    applyOrderingWithLimit(rawResult, statement, schema)
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

object ShardReaderActor {

  def props(basePath: String, db: String, namespace: String): Props =
    Props(new ShardReaderActor(basePath, db, namespace))
}
