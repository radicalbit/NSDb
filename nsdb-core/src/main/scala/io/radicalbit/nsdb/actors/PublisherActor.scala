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
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Cancellable, Props}
import akka.dispatch.ControlMessage
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Commands._
import io.radicalbit.nsdb.actors.PublisherActor.Events.Unsubscribed
import io.radicalbit.nsdb.actors.PublisherActor.{GraceTemporalBucketKey, NSDbQuery, TemporalBucket}
import io.radicalbit.nsdb.actors.RealTimeProtocol.Events.{
  RecordsPublished,
  SubscribedByQueryString,
  SubscriptionByQueryStringFailed
}
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel.precision
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.index.TemporaryIndex
import io.radicalbit.nsdb.model.{Schema, TimeContext}
import io.radicalbit.nsdb.post_proc.reduceSingleTemporalBucket
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser.{ParsedQuery, ParsedSimpleQuery, ParsedTemporalAggregatedQuery}
import io.radicalbit.nsdb.util.ActorPathLogging
import org.apache.lucene.index.IndexWriter

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

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
  lazy val plainQueries: mutable.Map[String, NSDbQuery] = mutable.Map.empty

  /**
    * mutable map of non aggregated queries by query id
    */
  lazy val temporalAggregatedQueries: mutable.Map[String, NSDbQuery] = mutable.Map.empty

  /**
    * mutable map of aggregated queries by query id
    */
  lazy val aggregatedQueries: mutable.Map[String, NSDbQuery] = mutable.Map.empty

  /**
    * Mutable map of schemas (the key is the metric name)
    */
  lazy val schemas: mutable.Map[String, Schema] = mutable.Map.empty

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val mathContext: MathContext =
    new MathContext(context.system.settings.config.getInt(precision), RoundingMode.HALF_UP)

  val interval: FiniteDuration = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.publisher.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  /**
    * Task for standard aggregated queries.
    */
  private var aggregatedPushTask: Cancellable = _

  /**
    * Tasks for temporal aggregated queries.
    */
  lazy val temporalAggregatedTasks: mutable.Map[String, Cancellable] = mutable.Map.empty

  /**
    * Temporal buckets grouped by quid.
    */
  lazy val currentTemporalBuckets: mutable.Map[String, TemporalBucket] = mutable.Map.empty

  /**
    *
    */
  lazy val graceTemporalBuckets: mutable.Map[GraceTemporalBucketKey, TemporalBucket] = mutable.Map.empty

  /**
    *
    */
  lazy val lateEventsToCheck: mutable.Map[String, Seq[Bit]] = mutable.Map.empty

  override def preStart(): Unit = {

    /**
      * scheduler that updates aggregated queries subscribers
      */
    aggregatedPushTask =
      context.system.scheduler.scheduleAtFixedRate(interval, interval, self, PushStandardAggregatedQueries)
  }

  override def receive: Receive = {
    case SubscribeBySqlStatement(actor, db, namespace, metric, queryString, query, timeContextOpt) =>
      val subscribedQueryId = (plainQueries ++ aggregatedQueries).collectFirst {
        case (_, v) if v.query == query => v.uuid
      } getOrElse
        UUID.randomUUID().toString

      implicit val timeContext: TimeContext = timeContextOpt getOrElse TimeContext()

      subscribedActorsByQueryId
        .get(subscribedQueryId)
        .find(_.contains(actor))
        .fold {
          log.debug(s"subscribing a new actor to query $queryString")
          (readCoordinator ? ExecuteStatement(query))
            .map {
              case SelectStatementExecuted(statement, values, schema) =>
                val previousRegisteredActors = subscribedActorsByQueryId.getOrElse(subscribedQueryId, Set.empty)
                schemas += (statement.metric                    -> schema)
                subscribedActorsByQueryId += (subscribedQueryId -> (previousRegisteredActors + actor))
                StatementParser.parseStatement(statement, schema) match {
                  case Right(parsedSimpleQuery: ParsedSimpleQuery) =>
                    plainQueries += (subscribedQueryId -> new NSDbQuery(subscribedQueryId,
                                                                        query,
                                                                        parsedSimpleQuery,
                                                                        timeContext))
                    SubscribedByQueryString(db, namespace, metric, queryString, subscribedQueryId, values)

                  case Right(parsedTemporalAggregatedQuery: ParsedTemporalAggregatedQuery) =>
                    temporalAggregatedQueries += (subscribedQueryId -> new NSDbQuery(subscribedQueryId,
                                                                                     query,
                                                                                     parsedTemporalAggregatedQuery,
                                                                                     timeContext))
                    if (!temporalAggregatedTasks.contains(subscribedQueryId)) {
                      val duration = FiniteDuration(parsedTemporalAggregatedQuery.interval,
                                                    java.util.concurrent.TimeUnit.MILLISECONDS)
                      temporalAggregatedTasks += (subscribedQueryId -> context.system.scheduler.scheduleAtFixedRate(
                        duration,
                        duration,
                        self,
                        PushTemporalAggregatedQueries(subscribedQueryId)))
                    }
                    SubscribedByQueryString(db, namespace, metric, queryString, subscribedQueryId, values)
                  case Right(parsedQuery: ParsedQuery) =>
                    aggregatedQueries += (subscribedQueryId -> new NSDbQuery(subscribedQueryId,
                                                                             query,
                                                                             parsedQuery,
                                                                             timeContext))
                    schemas += (statement.metric -> schema)
                    SubscribedByQueryString(db, namespace, metric, queryString, subscribedQueryId, values)
                  case Left(error) =>
                    SubscriptionByQueryStringFailed(db, namespace, metric, queryString, error)
                }
              case SelectStatementFailed(_, reason, _) =>
                SubscriptionByQueryStringFailed(db, namespace, metric, queryString, reason)
            }
            .pipeTo(sender())
        } { _ =>
          log.debug(s"subscribing existing actor to query $queryString")
          (readCoordinator ? ExecuteStatement(query))
            .mapTo[ExecuteSelectStatementResponse]
            .map {
              case e: SelectStatementExecuted =>
                SubscribedByQueryString(db, namespace, metric, queryString, subscribedQueryId, e.values)
              case e: SelectStatementFailed =>
                SubscriptionByQueryStringFailed(db, namespace, metric, queryString, e.reason)
            }
            .pipeTo(sender())
        }
    case PushStandardAggregatedQueries =>
      aggregatedQueries.foreach {
        case (id, q) if subscribedActorsByQueryId.get(id).exists(_.nonEmpty) =>
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

    case PushTemporalAggregatedQueries(quid) =>
      val timeContext = TimeContext()
      for {
        temporalQuery  <- temporalAggregatedQueries.get(quid)
        temporalBucket <- currentTemporalBuckets.get(quid)
        schema         <- schemas.get(temporalQuery.query.metric)
      } yield {
        temporalQuery.parsedQuery match {
          case parsedTemporalAggregatedQuery: ParsedTemporalAggregatedQuery =>
            reduceSingleTemporalBucket(schema, parsedTemporalAggregatedQuery.aggregation)(mathContext)(
              temporalBucket.bits).foreach { record =>
              subscribedActorsByQueryId
                .get(quid)
                .foreach(e => e.foreach(_ ! RecordsPublished(quid, schema.metric, Seq(record))))

              if (temporalBucket.lowerBound >= timeContext.currentTime - parsedTemporalAggregatedQuery.gracePeriod
                    .getOrElse(0L))
                graceTemporalBuckets += (GraceTemporalBucketKey(quid,
                                                                temporalBucket.lowerBound,
                                                                temporalBucket.upperBound) -> temporalBucket)

              graceTemporalBuckets --= graceTemporalBuckets.filter {
                case (key, _) =>
                  key.upperBound < timeContext.currentTime - parsedTemporalAggregatedQuery.gracePeriod.getOrElse(0L)
              }.keys

              currentTemporalBuckets -= quid

              lateEventsToCheck.getOrElse(quid, Seq.empty).foreach { lateEvent =>
                graceTemporalBuckets
                  .find {
                    case (key, _) => key.contains(lateEvent.timestamp)
                  }
                  .foreach {
                    case (_, lateBucketToPush) =>
                      reduceSingleTemporalBucket(schema, parsedTemporalAggregatedQuery.aggregation)(mathContext)(
                        lateBucketToPush.bits).foreach { record =>
                        subscribedActorsByQueryId
                          .get(quid)
                          .foreach(e => e.foreach(_ ! RecordsPublished(quid, schema.metric, Seq(record))))
                      }
                  }

              }
            }
          case _ =>
            log.warning(s"trying to process a non temporal query ${temporalQuery.query} as temporal query")
        }
      }

    case PublishRecord(db, namespace, metric, record, schema) =>
      def coordinatesMatch(nsdbQuery: NSDbQuery, db: String, namespace: String, metric: String) =
        db == nsdbQuery.query.db && namespace == nsdbQuery.query.namespace && metric == nsdbQuery.query.metric

      (plainQueries ++ temporalAggregatedQueries).foreach {
        case (quid, nsdbQuery) if nsdbQuery.query.metric == metric && subscribedActorsByQueryId.contains(quid) =>
          implicit val timeContext: TimeContext = nsdbQuery.timeContext

          val temporaryIndex: TemporaryIndex = new TemporaryIndex()
          implicit val writer: IndexWriter   = temporaryIndex.getWriter
          temporaryIndex.write(record)
          writer.close()

          nsdbQuery.parsedQuery match {
            case parsedQuery: ParsedSimpleQuery =>
              if (coordinatesMatch(nsdbQuery, db, namespace, metric) && temporaryIndex
                    .query(schema, parsedQuery.q, parsedQuery.fields, 1, None)
                    .lengthCompare(1) == 0)
                subscribedActorsByQueryId
                  .get(quid)
                  .foreach(e => e.foreach(_ ! RecordsPublished(quid, metric, Seq(record))))
              temporaryIndex.close()
            case parsedQuery: ParsedTemporalAggregatedQuery =>
              if (coordinatesMatch(nsdbQuery, db, namespace, metric) && temporaryIndex
                    .query(schema, parsedQuery.q, Seq.empty, 1, None)
                    .lengthCompare(1) == 0)
                currentTemporalBuckets
                  .get(quid)
                  .fold(
                    if (record.timestamp >= timeContext.currentTime)
                      currentTemporalBuckets += (quid -> TemporalBucket(timeContext.currentTime,
                                                                        timeContext.currentTime + parsedQuery.interval,
                                                                        ListBuffer(record)))
                    else if (record.timestamp >= timeContext.currentTime - parsedQuery.gracePeriod.getOrElse(0L))
                      lateEventsToCheck
                        .get(quid)
                        .fold(
                          lateEventsToCheck += (quid -> Seq(record))
                        ) { previousList =>
                          lateEventsToCheck += (quid -> (previousList :+ record))
                        } else
                      ()
                  ) { previousBucket =>
                    if (record.timestamp >= timeContext.currentTime)
                      currentTemporalBuckets += (quid -> previousBucket.copy(bits = previousBucket.bits :+ record))
                    else if (record.timestamp >= timeContext.currentTime - parsedQuery.gracePeriod.getOrElse(0L)) {
                      lateEventsToCheck
                        .get(quid)
                        .fold(
                          lateEventsToCheck += (quid -> Seq(record))
                        ) { previousList =>
                          lateEventsToCheck += (quid -> (previousList :+ record))
                        }
                    } else
                      ()
                  }
            case _ => // do nothing
          }
        case _ => // do nothing
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

  override def postStop(): Unit = {
    Option(aggregatedPushTask).foreach(_.cancel())
    temporalAggregatedTasks.foreach { case (_, task) => task.cancel() }
  }

}

object PublisherActor {

  def props(readCoordinator: ActorRef): Props =
    Props(new PublisherActor(readCoordinator))

  /**
    * Models queries used for the  subscription process.
    * @param uuid a generated, unique identifier for a query.
    * @param query the parsed select statement.
    * @param parsedQuery the parsed query which contains information about the type of query and the actual lucene query.
    * @param timeContext the time context to use for publishing records.
    */
  class NSDbQuery(val uuid: String,
                  val query: SelectSQLStatement,
                  val parsedQuery: ParsedQuery,
                  val timeContext: TimeContext)

  /**
    * Defines a sequence of partial results delimited by a time interval.
    */
  case class TemporalBucket(lowerBound: Long, upperBound: Long, bits: Seq[Bit]) {
    def contains(timestamp: Long): Boolean = timestamp <= upperBound && timestamp >= lowerBound
  }

  /**
    * Defines a key for a subscribed query within a time interval.
    */
  case class GraceTemporalBucketKey(quid: String, lowerBound: Long, upperBound: Long) {
    def contains(timestamp: Long): Boolean = timestamp <= upperBound && timestamp >= lowerBound
  }

  object Commands {
    case class SubscribeBySqlStatement(actor: ActorRef,
                                       db: String,
                                       namespace: String,
                                       metric: String,
                                       queryString: String,
                                       query: SelectSQLStatement,
                                       timeContext: Option[TimeContext] = None)
        extends ControlMessage
        with NSDbSerializable
    case class Unsubscribe(actor: ActorRef) extends ControlMessage with NSDbSerializable

    case class PushTemporalAggregatedQueries(quid: String) extends ControlMessage with NSDbSerializable
    case object PushStandardAggregatedQueries              extends ControlMessage with NSDbSerializable
  }

  object Events {
    case class Unsubscribed(actor: ActorRef) extends ControlMessage with NSDbSerializable
  }
}
