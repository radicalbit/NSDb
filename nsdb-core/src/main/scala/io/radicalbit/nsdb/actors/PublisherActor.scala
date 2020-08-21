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

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.dispatch.ControlMessage
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Commands.{SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events.{SubscribedByQueryStringInternal, Unsubscribed}
import io.radicalbit.nsdb.actors.RealTimeProtocol.Events.{RecordsPublished, SubscriptionByQueryStringFailed}
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}
import io.radicalbit.nsdb.common.statement.{SelectSQLStatement, SimpleGroupByAggregation, TemporalGroupByAggregation}
import io.radicalbit.nsdb.index.TemporaryIndex
import io.radicalbit.nsdb.model.TimeContext
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser.ParsedSimpleQuery
import io.radicalbit.nsdb.util.ActorPathLogging
import org.apache.lucene.index.IndexWriter

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * Models queries used for the  subscription process.
  * @param uuid a generated, unique identifier for a query.
  * @param query the parsed select statement.
  */
case class NSDbQuery(uuid: String, query: SelectSQLStatement) {
  val aggregated: Boolean = query.groupBy.nonEmpty

  val simpleAggregated: Boolean = query.groupBy.exists(_.isInstanceOf[SimpleGroupByAggregation])

  val temporalAggregated: Boolean = query.groupBy.exists(_.isInstanceOf[TemporalGroupByAggregation])
}

/**
  * Actor responsible to accept subscriptions and publish events to its subscribers.
  * A subscription can be achieved by sending:
  *
  * - [[SubscribeBySqlStatement]] the subscription is asked providing a query string, which is parsed into [[SelectSQLStatement]]. If the query does not exist, a new one will be created and registered.
  *
  * A generic publisher must send a [[PublishRecord]] message in order to trigger the publishing mechanism.
  * Every published event will be checked against every registered query. If the event fulfills the query, it will be published to every query relatedsubscriber.
  * @param readCoordinator global read coordinator responsible to execute queries when needed
  */
class PublisherActor(readCoordinator: ActorRef) extends ActorPathLogging {

  import context.dispatcher

  /**
    * mutable subscriber map aggregated by query id
    */
  lazy val subscribedActorsByQueryId: mutable.Map[String, Set[ActorRef]] = mutable.Map.empty

  /**
    * mutable map of non aggregated queries by query id
    */
  lazy val queries: mutable.Map[String, NSDbQuery] = mutable.Map.empty

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  val interval: FiniteDuration = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.publisher.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  /**
    * scheduler that updates aggregated queries subscribers
    */
  context.system.scheduler.schedule(interval, interval) {
    queries.foreach {
      case (id, q) if q.aggregated && subscribedActorsByQueryId.get(id).exists(_.nonEmpty) =>
        val f = (readCoordinator ? ExecuteStatement(q.query))
          .map {
            case e: SelectStatementExecuted => RecordsPublished(id, e.statement.metric, e.values)
            case SelectStatementFailed(statement, reason, _) =>
              log.error(s"aggregated statement {} subscriber refresh failed because of {}", statement, reason)
              RecordsPublished(id, q.query.metric, Seq.empty)
          }
        subscribedActorsByQueryId.get(id).foreach(e => e.foreach(f.pipeTo(_)))
      case _ => //do nothing
    }
  }

  override def receive: Receive = {
    case SubscribeBySqlStatement(actor, db, namespace, metric, queryString, query) =>
      val subscribedQueryId = queries.find { case (_, v) => v.query == query }.map(_._1) getOrElse
        UUID.randomUUID().toString

      subscribedActorsByQueryId
        .get(subscribedQueryId)
        .find(_.contains(actor))
        .fold {
          log.debug(s"subscribing a new actor to query $queryString")
          (readCoordinator ? ExecuteStatement(query))
            .map {
              case e: SelectStatementExecuted =>
                val previousRegisteredActors = subscribedActorsByQueryId.getOrElse(subscribedQueryId, Set.empty)
                subscribedActorsByQueryId += (subscribedQueryId -> (previousRegisteredActors + actor))
                queries += (subscribedQueryId                   -> NSDbQuery(subscribedQueryId, query))
                SubscribedByQueryStringInternal(db, namespace, metric, queryString, subscribedQueryId, e.values)
              case SelectStatementFailed(_, reason, _) =>
                SubscriptionByQueryStringFailed(db, namespace, metric, queryString, reason)
            }
            .pipeTo(sender())
        } { _ =>
          log.debug(s"subscribing existing actor to query $queryString")
          (readCoordinator ? ExecuteStatement(query))
            .mapTo[SelectStatementExecuted]
            .map(e => SubscribedByQueryStringInternal(db, namespace, metric, queryString, subscribedQueryId, e.values))
            .pipeTo(sender())
        }
    case PublishRecord(db, namespace, metric, record, schema) =>
      queries.foreach {
        case (id, nsdbQuery)
            if !nsdbQuery.aggregated && nsdbQuery.query.metric == metric && subscribedActorsByQueryId.contains(id) =>
          implicit val timeContext: TimeContext = TimeContext()
          val luceneQuery                       = StatementParser.parseStatement(nsdbQuery.query, schema)
          luceneQuery match {
            case Right(parsedQuery: ParsedSimpleQuery) =>
              val temporaryIndex: TemporaryIndex = new TemporaryIndex()
              implicit val writer: IndexWriter   = temporaryIndex.getWriter
              temporaryIndex.write(record)
              writer.close()
              if (db == nsdbQuery.query.db && namespace == nsdbQuery.query.namespace && metric == nsdbQuery.query.metric && temporaryIndex
                    .query(schema, parsedQuery.q, parsedQuery.fields, 1, None)
                    .lengthCompare(1) == 0)
                subscribedActorsByQueryId
                  .get(id)
                  .foreach(e => e.foreach(_ ! RecordsPublished(id, metric, Seq(record))))
              temporaryIndex.close()
            case Right(_) => log.error("unreachable branch reached...")
            case Left(error) =>
              log.error(s"query ${nsdbQuery.query} against schema $schema not valid because $error")
          }
        case _ =>
      }
    case Unsubscribe(actor) =>
      log.debug("unsubscribe actor {} ", actor)
      subscribedActorsByQueryId.foreach {
        case (k, v) if v.contains(actor) =>
          if (v.size == 1) subscribedActorsByQueryId -= k
          else
            subscribedActorsByQueryId += (k -> (v - actor))
        case _ => //do nothing
      }
      sender() ! Unsubscribed(actor)
  }
}

object PublisherActor {

  def props(readCoordinator: ActorRef): Props =
    Props(new PublisherActor(readCoordinator))

  object Commands {
    case class SubscribeBySqlStatement(actor: ActorRef,
                                       db: String,
                                       namespace: String,
                                       metric: String,
                                       queryString: String,
                                       query: SelectSQLStatement)
        extends ControlMessage
        with NSDbSerializable
    case class Unsubscribe(actor: ActorRef) extends ControlMessage with NSDbSerializable
  }

  object Events {
    case class Unsubscribed(actor: ActorRef) extends ControlMessage with NSDbSerializable
    case class SubscribedByQueryStringInternal(db: String,
                                               namespace: String,
                                               metric: String,
                                               queryString: String,
                                               quid: String,
                                               records: Seq[Bit])
        extends ControlMessage
        with NSDbSerializable
  }
}
