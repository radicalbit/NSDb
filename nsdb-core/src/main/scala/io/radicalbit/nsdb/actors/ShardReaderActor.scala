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
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.CountAllGroupsCollector
import io.radicalbit.nsdb.index.{FacetIndex, TimeSeriesIndex}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteSelectStatement, GetCount}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{CountGot, SelectStatementExecuted, SelectStatementFailed}
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser.{ParsedAggregatedQuery, ParsedSimpleQuery}
import org.apache.lucene.index.IndexNotFoundException
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.store.MMapDirectory

import scala.util.{Failure, Success, Try}

class ShardReaderActor(val basePath: String, val db: String, val namespace: String, val shardKey: ShardKey)
    extends Actor {

  lazy val directory =
    new MMapDirectory(
      Paths.get(basePath, db, namespace, "shards", s"${shardKey.metric}_${shardKey.from}_${shardKey.to}"))
  lazy val index = new TimeSeriesIndex(directory)

  lazy val facetDirectory =
    new MMapDirectory(
      Paths.get(basePath, db, namespace, "shards", s"${shardKey.metric}_${shardKey.from}_${shardKey.to}", "facet"))
  lazy val taxoDirectory = new MMapDirectory(
    Paths
      .get(basePath, db, namespace, "shards", s"${shardKey.metric}_${shardKey.from}_${shardKey.to}", "facet", "taxo"))
  lazy val facetIndex = new FacetIndex(facetDirectory, taxoDirectory)

  private val statementParser = new StatementParser()

  private def handleQueryResults(out: Try[Seq[Bit]]): Try[Seq[Bit]] = {
    out.recoverWith {
      case _: IndexNotFoundException => Success(Seq.empty)
    }
  }

  override def receive: Receive = {
    case GetCount(_, _, metric) =>
      handleQueryResults(Try(index.query(new MatchAllDocsQuery(), Seq.empty, Int.MaxValue, None)(identity))) match {
        case Success(bits) =>
          sender ! CountGot(db, namespace, metric, bits.size)
        case _ => sender ! CountGot(db, namespace, metric, 0)
      }
    case ExecuteSelectStatement(statement, schema) =>
      statementParser.parseStatement(statement, schema) match {
        case Success(ParsedSimpleQuery(_, _, q, false, limit, fields, sort)) =>
          handleQueryResults(Try(index.query(q, fields, limit, sort)(identity))) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
          }
        case Success(ParsedSimpleQuery(_, _, q, true, limit, fields, sort)) if fields.lengthCompare(1) == 0 =>
          handleQueryResults(Try(facetIndex.getDistinctField(q, fields.map(_.name).head, sort, limit))) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) =>
              sender ! SelectStatementFailed(ex.getMessage)
          }
        case Success(ParsedAggregatedQuery(_, _, q, collector: CountAllGroupsCollector[_], sort, limit)) =>
          handleQueryResults(Try(facetIndex
            .getCount(q, collector.groupField, sort, limit, schema.fieldsMap(collector.groupField).indexType))) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
          }
        case Success(ParsedAggregatedQuery(_, _, q, collector, sort, limit)) =>
          handleQueryResults(Try(index.query(q, collector.clear, limit, sort))) match {
            case Success(bits) =>
              sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
            case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
          }
        case Success(_)  => sender ! SelectStatementFailed("Unsupported query type")
        case Failure(ex) => sender ! SelectStatementFailed(ex.getMessage)
      }
    case RefreshShard =>
      index.refresh()
      facetIndex.refresh()
  }
}

object ShardReaderActor {
  case object DeleteAll
  case object RefreshShard

  def props(basePath: String, db: String, namespace: String, shardKey: ShardKey): Props =
    Props(new ShardReaderActor(basePath, db, namespace, shardKey))
}
