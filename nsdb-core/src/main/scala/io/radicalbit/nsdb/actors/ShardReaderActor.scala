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
import io.radicalbit.nsdb.actors.ShardReaderActor.{PrimaryAggregationResults, RefreshShard}
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.common.configuration.NSDbConfig
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.index.lucene.Index.handleNoIndexResults
import io.radicalbit.nsdb.model.{Location, Schema}
import io.radicalbit.nsdb.post_proc._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteSelectStatement, GetCountWithLocations}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{CountGot, SelectStatementExecuted, SelectStatementFailed}
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.util.ActorPathLogging
import org.apache.lucene.index.IndexNotFoundException
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
                                 schema: Schema): Try[PrimaryAggregationResults] = {

    implicit val numeric: Numeric[Any] = schema.value.indexType.asNumericType.numeric

    val distinctPrimaryAggregations = aggregations.flatMap {
      case primary: PrimaryAggregation   => Seq(primary)
      case composite: DerivedAggregation => composite.primaryAggregationsRequired
    }.distinct

    (for {
      (groupField, groupFieldSchemaField) <- Try(
        schema.tags.headOption.getOrElse(Schema.timestampField -> schema.timestamp))
      quantities <- distinctPrimaryAggregations
        .foldLeft(Try(Map.empty[String, NSDbNumericType])) {
          case (tryAcc, _: CountAggregation) =>
            tryAcc.map(acc => acc + (`count(*)` -> NSDbNumericType(index.getCount(query))))
          case (tryAcc, _: SumAggregation) =>
            tryAcc.map { acc =>
              val sum = facetIndexes
                .executeSumFacet(query, groupField, None, None, groupFieldSchemaField.indexType, schema.value.indexType)
                .map(_.value.rawValue)
                .sum
              acc + (`sum(*)` -> NSDbNumericType(sum))
            }
          case (tryAcc, _: MinAggregation) =>
            tryAcc.map { acc =>
              val minPerGroup = index.getMinGroupBy(query, schema, groupField)
              val minCrossGroup =
                minPerGroup.reduceLeftOption((bitL, bitR) => if (bitR.value <= bitL.value) bitR else bitL)
              minCrossGroup.fold(acc)(min => acc + (`min(*)` -> min.value))
            }
          case (tryAcc, _) => tryAcc
        }
      uniqueValues <- {
        distinctPrimaryAggregations
          .collectFirst {
            case aggregation: CountDistinctAggregation =>
              Try(index.uniqueValues(query, schema, groupField, aggregation.fieldName).flatMap(_.uniqueValues).toSet)
          }
          .getOrElse(Success(Set.empty[NSDbType]))
      }
    } yield PrimaryAggregationResults(quantities, uniqueValues)).recoverWith {
      case _: IndexNotFoundException => Success(PrimaryAggregationResults(Map.empty, Set.empty))
    }
  }

  /**
    * Computes all the primary aggregations with the related zero values
    * @param aggregations the aggregations to compute.
    * @param schema metric schema.
    * @return a map containing the results of the aggregations.
    */
  def computeZeroPrimaryAggregations(aggregations: Seq[Aggregation], schema: Schema): Map[String, NSDbNumericType] = {
    val valueNumericType               = schema.value.indexType.asNumericType
    implicit val numeric: Numeric[Any] = valueNumericType.numeric
    val distinctPrimaryAggregations = aggregations.flatMap {
      case primary: PrimaryAggregation   => Seq(primary)
      case composite: DerivedAggregation => composite.primaryAggregationsRequired
    }.distinct

    distinctPrimaryAggregations.collect {
      case _: CountAggregation => `count(*)` -> NSDbNumericType(0L)
      case _: SumAggregation   => `sum(*)`   -> NSDbNumericType(numeric.zero)
      case _: MinAggregation   => `min(*)`   -> valueNumericType.MAX_VALUE
    }.toMap
  }

  override def receive: Receive = {
    case GetCountWithLocations(_, _, metric, _) =>
      val count = Try { index.getCount() }.recover { case _ => 0L }.getOrElse(0L)
      sender ! CountGot(db, namespace, metric, count)
    case ReceiveTimeout =>
      self ! PoisonPill
    case ExecuteSelectStatement(statement, schema, _, timeRangeContext, timeContext, _) =>
      log.debug("executing statement in metric shard reader actor {}", statement)
      val results: Try[Seq[Bit]] = StatementParser.parseStatement(statement, schema)(timeContext) match {
        case Right(ParsedSimpleQuery(_, _, q, false, limit, fields, sort)) =>
          statement.getTimeOrdering match {
            case Some(_) =>
              handleNoIndexResults(Try(index.query(schema, q, fields, limit, sort)))
            case None =>
              if (limit == Int.MaxValue) handleNoIndexResults(Try(index.query(schema, q, fields, limit, None)))
              else handleNoIndexResults(Try(index.query(schema, q, fields, limit, sort)))
          }
        case Right(ParsedSimpleQuery(_, _, q, true, _, fields, _)) if fields.lengthCompare(1) == 0 =>
          handleNoIndexResults(Try(facetIndexes.executeDistinctFieldCountIndex(q, fields.map(_.name).head, None)))
        case Right(ParsedGlobalAggregatedQuery(_, _, q, limit, fields, aggregations, sort)) =>
          val localPrimaryAggregationsResults = computePrimaryAggregations(aggregations, q, schema)
          if (fields.nonEmpty) {
            for {
              localPrimaryAggregations <- localPrimaryAggregationsResults
              results                  <- handleNoIndexResults(Try(index.query(schema, q, fields, limit, sort)))
            } yield {
              /*
              In case there are aggregations mixed into a plain resultset we must ensure that we're not compute
              the aggregations more than once in the post processing phase. As per the following code
              we make sure that the aggregations are set only in the head of the resultset (if present).
               */

              val headWithAggregation = results.headOption.toSeq
                .map(
                  bit =>
                    bit.copy(tags = bit.tags ++ localPrimaryAggregations.quantities,
                             uniqueValues = localPrimaryAggregations.uniqueValues))
              val tailWithoutAggregation = results
                .drop(1)
                .map(bit => bit.copy(tags = bit.tags ++ computeZeroPrimaryAggregations(aggregations, schema)))
              headWithAggregation ++ tailWithoutAggregation
            }
          } else {
            localPrimaryAggregationsResults.map { primaryAggregations =>
              val aggregationTags = primaryAggregations.quantities
              val uniqueValues    = primaryAggregations.uniqueValues
              Seq(Bit(0, 0L, Map.empty, aggregationTags, uniqueValues))
            }
          }
        case Right(
            ParsedAggregatedQuery(_, _, q, InternalStandardAggregation(groupField, _: CountAggregation), _, limit)) =>
          handleNoIndexResults(
            Try(facetIndexes.executeCountFacet(q, groupField, None, limit, schema.fieldsMap(groupField).indexType)))

        case Right(
            ParsedAggregatedQuery(_,
                                  _,
                                  q,
                                  InternalStandardAggregation(groupField, aggregation: CountDistinctAggregation),
                                  _,
                                  _)) =>
          handleNoIndexResults(
            Try(index.uniqueValues(q, schema, groupField, aggregation.fieldName))
          )
        case Right(
            ParsedAggregatedQuery(_, _, q, InternalStandardAggregation(groupField, _: SumAggregation), _, limit)) =>
          handleNoIndexResults(
            Try(
              facetIndexes.executeSumFacet(q,
                                           groupField,
                                           None,
                                           limit,
                                           schema.fieldsMap(groupField).indexType,
                                           schema.value.indexType)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalStandardAggregation(groupField, _: AvgAggregation), _, _)) =>
          handleNoIndexResults(
            Try(
              facetIndexes.executeSumAndCountFacet(q,
                                                   groupField,
                                                   None,
                                                   schema.fieldsMap(groupField).indexType,
                                                   schema.value.indexType)))
        case Right(
            ParsedAggregatedQuery(_, _, q, InternalStandardAggregation(groupField, _: FirstAggregation), _, _)) =>
          handleNoIndexResults(Try(index.getFirstGroupBy(q, schema, groupField)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalStandardAggregation(groupField, _: LastAggregation), _, _)) =>
          handleNoIndexResults(Try(index.getLastGroupBy(q, schema, groupField)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalStandardAggregation(groupField, _: MaxAggregation), _, _)) =>
          handleNoIndexResults(Try(index.getMaxGroupBy(q, schema, groupField)))
        case Right(ParsedAggregatedQuery(_, _, q, InternalStandardAggregation(groupField, _: MinAggregation), _, _)) =>
          handleNoIndexResults(Try(index.getMinGroupBy(q, schema, groupField)))
        case Right(
            ParsedTemporalAggregatedQuery(_,
                                          _,
                                          q,
                                          _,
                                          InternalTemporalAggregation(countAggregation: CountAggregation),
                                          _,
                                          _,
                                          _,
                                          _)) =>
          handleNoIndexResults(
            Try(
              facetIndexes
                .executeRangeFacet(
                  index.getSearcher,
                  q,
                  InternalTemporalAggregation(countAggregation),
                  "timestamp",
                  "value",
                  None,
                  timeRangeContext
                    .map(_.ranges)
                    .getOrElse(Seq.empty)
                    .filter(_.intersect(location))
                )))

        case Right(
            ParsedTemporalAggregatedQuery(_,
                                          _,
                                          q,
                                          interval,
                                          InternalTemporalAggregation(
                                            countDistinctAggregation: CountDistinctAggregation),
                                          _,
                                          _,
                                          _,
                                          _)) =>
          val filteredRanges = timeRangeContext
            .map(_.ranges)
            .getOrElse(Seq.empty)
            .filter(_.intersect(location))

          if (filteredRanges.isEmpty) Try(Seq.empty[Bit])
          else
            handleNoIndexResults(
              Try(
                index.uniqueRangeValues(
                  q,
                  schema,
                  countDistinctAggregation.fieldName,
                  filteredRanges.lastOption.map(_.lowerBound).getOrElse(0L),
                  interval,
                  filteredRanges.headOption.map(_.upperBound).getOrElse(0L)
                )))

        case Right(ParsedTemporalAggregatedQuery(_, _, q, _, aggregationType, _, _, _, _)) =>
          val valueFieldType: IndexType[_] = schema.value.indexType
          handleNoIndexResults(
            Try(
              facetIndexes
                .executeRangeFacet(
                  index.getSearcher,
                  q,
                  aggregationType,
                  "timestamp",
                  "value",
                  Some(valueFieldType),
                  timeRangeContext.map(_.ranges).getOrElse(Seq.empty).filter(_.intersect(location))
                )))
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
          sender ! SelectStatementExecuted(statement, bits, schema)
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

  final val maxGroupResults: Int = 10000

  case object RefreshShard extends NSDbSerializable

  /**
    * Stores the results from primary aggregations
    * @param quantities single quantities (count, sum etc.).
    * @param uniqueValues unique values that will be used for count distinct calculation.
    */
  case class PrimaryAggregationResults(quantities: Map[String, NSDbNumericType], uniqueValues: Set[NSDbType])

  def props(basePath: String, db: String, namespace: String, location: Location): Props =
    Props(new ShardReaderActor(basePath, db, namespace, location))
}
