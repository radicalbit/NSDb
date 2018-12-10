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

import akka.actor.{Actor, Props}
import io.radicalbit.nsdb.actors.ShardReaderActor.RefreshShard
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.Index.handleNoIndexResults
import io.radicalbit.nsdb.index.{AllFacetIndexes, TimeSeriesIndex}
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteSelectStatement, GetCount}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{CountGot, SelectStatementExecuted, SelectStatementFailed}
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser._
import org.apache.lucene.store.MMapDirectory

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
    extends Actor {

  lazy val directory =
    new MMapDirectory(
      Paths.get(basePath, db, namespace, "shards", s"${location.metric}_${location.from}_${location.to}"))

  lazy val index = new TimeSeriesIndex(directory)

  lazy val facetIndexes = new AllFacetIndexes(basePath = basePath, db = db, namespace = namespace, location = location)

  override def receive: Receive = {
    case GetCount(_, _, metric) => sender ! CountGot(db, namespace, metric, index.getCount())

    case ExecuteSelectStatement(statement, schema, locations) =>
      StatementParser.parseStatement(statement, schema) match {
        case Success(ParsedSimpleQuery(_, _, q, false, limit, fields, sort)) =>
          handleNoIndexResults(Try(index.query(schema, q, fields, limit, sort)(identity))) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
          }

        case Success(ParsedSimpleQuery(_, _, q, true, limit, fields, sort)) if fields.lengthCompare(1) == 0 =>
          handleNoIndexResults(
            Try(facetIndexes.facetCountIndex.getDistinctField(q, fields.map(_.name).head, sort, limit))) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) =>
              sender ! SelectStatementFailed(ex.getMessage)
          }

        case Success(ParsedAggregatedQuery(_, _, q, InternalCountAggregation(groupField, _), sort, limit)) =>
          handleNoIndexResults(
            Try(
              facetIndexes.facetCountIndex
                .result(q, groupField, sort, limit, schema.fieldsMap(groupField).indexType))) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
          }

        case Success(ParsedAggregatedQuery(_, _, q, InternalSumAggregation(groupField, _), sort, limit)) =>
          handleNoIndexResults(
            Try(
              facetIndexes.facetSumIndex
                .result(q,
                        groupField,
                        sort,
                        limit,
                        schema.fieldsMap(groupField).indexType,
                        Some(schema.fieldsMap("value").indexType)))) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
          }

        case Success(ParsedTemporalAggregatedQuery(_, _, q, ranges, _, _)) =>
          handleNoIndexResults(Try(index.executeCountLongRangeFacet(index.getSearcher, q, "timestamp", ranges) {
            facetResult =>
              facetResult.labelValues.toSeq
                .map { lv =>
                  val boundaries = lv.label.split("-").map(_.toLong).toSeq
                  Bit(boundaries.head,
                      lv.value.longValue(),
                      Map[String, JSerializable](("lowerBound", boundaries.head),
                                                 ("upperBound", boundaries.tail.head)),
                      Map.empty)
                }
          })) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
          }

        case Success(ParsedAggregatedQuery(_, _, q, aggregationType, sort, limit)) =>
          sender ! SelectStatementFailed(s"$aggregationType is not currently supported.")

        case Success(_)  => sender ! SelectStatementFailed("Unsupported query type")
        case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
      }
    case RefreshShard =>
      index.refresh()
      facetIndexes.refresh()
  }
}

object ShardReaderActor {

  case object DeleteAll

  case object RefreshShard

  def props(basePath: String, db: String, namespace: String, location: Location): Props =
    Props(new ShardReaderActor(basePath, db, namespace, location))
}
