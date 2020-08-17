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

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{PoisonPill, Props, ReceiveTimeout}
import io.radicalbit.nsdb.actors.ShardReaderActor.RefreshShard
import io.radicalbit.nsdb.common.NSDbNumericType
import io.radicalbit.nsdb.common.configuration.NSDbConfig
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.index.lucene.Index.{handleNoIndexResults, handleNumericNoIndexResults}
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.post_proc._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteSelectStatement, GetCountWithLocations}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{CountGot, SelectStatementExecuted, SelectStatementFailed}
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.util.ActorPathLogging
import org.apache.lucene.search.Query
import org.apache.lucene.store.Directory

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Actor responsible to perform read operations for a single shard.
  *
  * @param basePath Index base path.
  * @param db the shard db.
  * @param namespace the shard namespace.
  * @param location the shard location.
  */
class ShardReaderActor(val basePath: String, val db: String, val namespace: String, val location: Location)
    extends ActorPathLogging
    with DirectorySupport {

  override lazy val indexStorageStrategy: StorageStrategy =
    StorageStrategy.withValue(context.system.settings.config.getString(NSDbConfig.HighLevel.StorageStrategy))

  lazy val directory: Directory =
    getDirectory(Paths.get(basePath, db, namespace, "shards", s"${location.shardName}"))

  lazy val index = new TimeSeriesIndex(directory)

  lazy val facetIndexes =
    new AllFacetIndexes(basePath = basePath, db = db, namespace = namespace, location = location, indexStorageStrategy)

  lazy val passivateAfter: FiniteDuration = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.sharding.passivate-after").toNanos,
    TimeUnit.NANOSECONDS)

  context.setReceiveTimeout(passivateAfter)

  /**
    * Computes all the primary aggregations
    * @param aggregations the aggregations to compute.
    * @param query the original lucene query.
    * @param schema metric schema.
    * @return a map containing the results of the aggregations.
    */
  def computePrimaryAggregations(aggregations: Seq[Aggregation],
                                 query: Query,
                                 schema: Schema): Try[Map[String, NSDbNumericType]] = {

    import io.radicalbit.nsdb.common.util.ErrorUtils._

    implicit val numeric = schema.value.indexType.asInstanceOf[NumericType[_]].numeric
    val distinctPrimaryAggregations = aggregations.flatMap {
      case primary: PrimaryAggregation   => Seq(primary)
      case composite: DerivedAggregation => composite.primaryAggregationsRequired
    }.distinct
    flatten(distinctPrimaryAggregations
      .map {
        case CountAggregation =>
          handleNumericNoIndexResults(Try(index.getCount(query))).map(count => `count(*)` -> NSDbNumericType(count))
        case SumAggregation =>
          val (groupField, groupFieldSchemaField) = schema.tags.head
          handleNumericNoIndexResults(
            Try(
              facetIndexes
                .executeSumFacet(query, groupField, None, None, groupFieldSchemaField.indexType, schema.value.indexType)
                .map(_.value.rawValue)
                .sum)).map(sum => `sum(*)` -> NSDbNumericType(sum))
      }).map(_.toMap)
  }

  /**
    * Computes all the primary aggregations with the related zero values
    * @param aggregations the aggregations to compute.
    * @param schema metric schema.
    * @return a map containing the results of the aggregations.
    */
  def computeZeroPrimaryAggregations(aggregations: Seq[Aggregation], schema: Schema): Map[String, NSDbNumericType] = {
    implicit val numeric = schema.value.indexType.asInstanceOf[NumericType[_]].numeric
    val distinctPrimaryAggregations = aggregations.flatMap {
      case primary: PrimaryAggregation   => Seq(primary)
      case composite: DerivedAggregation => composite.primaryAggregationsRequired
    }.distinct

    distinctPrimaryAggregations.map {
      case CountAggregation => "count(*)" -> NSDbNumericType(0L)
      case SumAggregation   => "sum(*)"   -> NSDbNumericType(numeric.zero)
    }.toMap
  }

  override def receive: Receive = {
    case GetCountWithLocations(_, _, metric, _) =>
      val count = Try { index.getCount() }.recover { case _ => 0L }.getOrElse(0L)
      sender ! CountGot(db, namespace, metric, count)
    case ReceiveTimeout =>
      self ! PoisonPill
    case ExecuteSelectStatement(statement, schema, _, globalRanges, _) =>
      log.debug("executing statement in metric shard reader actor {}", statement)
      val results: Try[Seq[Bit]] = StatementParser.parseStatement(statement, schema) match {
        case Right(ParsedSimpleQuery(_, _, q, false, limit, fields, sort)) =>
          statement.getTimeOrdering match {
            case Some(_) =>
              handleNoIndexResults(Try(index.query(schema, q, fields, limit, sort)))
            case None =>
              if (limit == Int.MaxValue) handleNoIndexResults(Try(index.query(schema, q, fields, limit, None)))
              else handleNoIndexResults(Try(index.query(schema, q, fields, limit, sort)))
          }
        case Right(ParsedSimpleQuery(_, _, q, true, limit, fields, _)) if fields.lengthCompare(1) == 0 =>
          handleNoIndexResults(
            Try(facetIndexes.executeDistinctFieldCountIndex(q, fields.map(_.name).head, None, limit)))
        case Right(ParsedGlobalAggregatedQuery(_, _, q, limit, fields, aggregations, sort)) =>
          val primaryAggregationsResults = computePrimaryAggregations(aggregations, q, schema)
          if (fields.nonEmpty) {
            for {
              primaryAggregations <- primaryAggregationsResults
              results             <- handleNoIndexResults(Try(index.query(schema, q, fields, limit, sort)))
            } yield {
              /*
              In case there are aggregations mixed into a plain resultset we must ensure that we're not compute the aggregations more than once
              in the post processing phase.
              As per the following code we make sure that the aggregations are set only in the head of the resultset (if present).
               */
              results.headOption.toSeq.flatMap(bit => Seq(bit.copy(tags = bit.tags ++ primaryAggregations))) ++
                results.tail.map(bit =>
                  bit.copy(tags = bit.tags ++ computeZeroPrimaryAggregations(aggregations, schema)))
            }
          } else {
            primaryAggregationsResults.map(primaryAggregations => Seq(Bit(0, 0L, Map.empty, primaryAggregations)))
          }
        case Right(ParsedAggregatedQuery(_, _, q, InternalCountStandardAggregation(groupField), _, limit)) =>
          handleNoIndexResults(
            Try(facetIndexes.executeCountFacet(q, groupField, None, limit, schema.fieldsMap(groupField).indexType)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalSumStandardAggregation(groupField), _, limit)) =>
          handleNoIndexResults(
            Try(
              facetIndexes.executeSumFacet(q,
                                           groupField,
                                           None,
                                           limit,
                                           schema.fieldsMap(groupField).indexType,
                                           schema.value.indexType)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalAvgStandardAggregation(groupField), _, _)) =>
          handleNoIndexResults(
            Try(
              facetIndexes.executeSumAndCountFacet(q,
                                                   groupField,
                                                   None,
                                                   schema.fieldsMap(groupField).indexType,
                                                   schema.value.indexType)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalFirstStandardAggregation(groupField), _, _)) =>
          handleNoIndexResults(Try(index.getFirstGroupBy(q, schema, groupField)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalLastStandardAggregation(groupField), _, _)) =>
          handleNoIndexResults(Try(index.getLastGroupBy(q, schema, groupField)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalMaxStandardAggregation(groupField), _, _)) =>
          handleNoIndexResults(Try(index.getMaxGroupBy(q, schema, groupField)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalMinStandardAggregation(groupField), _, _)) =>
          handleNoIndexResults(Try(index.getMinGroupBy(q, schema, groupField)))
        case Right(ParsedTemporalAggregatedQuery(_, _, q, _, InternalCountTemporalAggregation, _, _, _)) =>
          handleNoIndexResults(
            Try(
              facetIndexes
                .executeRangeFacet(index.getSearcher,
                                   q,
                                   InternalCountTemporalAggregation,
                                   "timestamp",
                                   "value",
                                   None,
                                   globalRanges.filter(_.intersect(location)))))
        case Right(ParsedTemporalAggregatedQuery(_, _, q, _, aggregationType, _, _, _)) =>
          val valueFieldType: IndexType[_] = schema.value.indexType
          handleNoIndexResults(
            Try(
              facetIndexes
                .executeRangeFacet(index.getSearcher,
                                   q,
                                   aggregationType,
                                   "timestamp",
                                   "value",
                                   Some(valueFieldType),
                                   globalRanges.filter(_.intersect(location)))))
        case Right(ParsedAggregatedQuery(_, _, _, aggregationType, _, _)) =>
          Failure(new RuntimeException(s"$aggregationType is not currently supported."))
        case Right(_) =>
          Failure(new RuntimeException("Unsupported query type"))
        case Left(error) =>
          log.error("error occurred executing query {} in location {} {}", statement, location, error)
          Failure(new RuntimeException(error))
      }
      results match {
        case Success(bits) =>
          sender ! SelectStatementExecuted(statement, bits)
        case Failure(ex) =>
          log.error(ex, "error occurred executing query {} in location {}", statement, location)
          sender ! SelectStatementFailed(statement, ex.getMessage)
      }
    case RefreshShard =>
      index.refresh()
      facetIndexes.refresh()
  }

  override def postStop(): Unit = {
    facetIndexes.close()
    directory.close()
  }
}

object ShardReaderActor {

  case object RefreshShard extends NSDbSerializable

  def props(basePath: String, db: String, namespace: String, location: Location): Props =
    Props(new ShardReaderActor(basePath, db, namespace, location))
}
