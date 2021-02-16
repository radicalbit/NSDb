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
import io.radicalbit.nsdb.actors.PublisherActor.{LateTemporalBucketKey, NSDbQuery, TemporalBucket}
import io.radicalbit.nsdb.protocol.RealTimeProtocol.Events.{
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
  lazy val temporalBuckets: mutable.Map[String, TemporalBucket] = mutable.Map.empty

  /**
    * Temporal buckets for late events, i.e. events that comes in the past but within a grace period (specified in the query statement).
    */
  lazy val lateTemporalBuckets: mutable.Map[LateTemporalBucketKey, TemporalBucket] = mutable.Map.empty

  /**
    * Buffer to store late events to check periodically.
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
      val subscribedQueryId = (plainQueries ++ aggregatedQueries ++ temporalAggregatedQueries).collectFirst {
        case (_, v) if v.query == query =>
          v.uuid
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
                      val intervalAsDuration = FiniteDuration(parsedTemporalAggregatedQuery.interval,
                                                              java.util.concurrent.TimeUnit.MILLISECONDS)
                      temporalAggregatedTasks += (subscribedQueryId -> context.system.scheduler.scheduleAtFixedRate(
                        intervalAsDuration,
                        intervalAsDuration,
                        self,
                        PushTemporalAggregatedQueries(subscribedQueryId, parsedTemporalAggregatedQuery.interval)))
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
              case e: SelectStatementExecuted =>
                RecordsPublished(id, e.statement.db, e.statement.namespace, e.statement.metric, e.values)
              case SelectStatementFailed(statement, reason, _) =>
                log.error(s"aggregated statement {} subscriber refresh failed because of {}", statement, reason)
                RecordsPublished(id, q.query.db, q.query.namespace, q.query.metric, Seq.empty)
            }
          subscribedActorsByQueryId.get(id).foreach(e => e.foreach(f.pipeTo(_)))
        case _ => //do nothing
      }

    case PushTemporalAggregatedQueries(quid, interval) =>
      for {
        temporalQuery <- temporalAggregatedQueries.get(quid)
        schema        <- schemas.get(temporalQuery.query.metric)
      } yield {

        val timeContext = temporalQuery.timeContext

        temporalQuery.parsedQuery match {
          case parsedTemporalAggregatedQuery: ParsedTemporalAggregatedQuery =>
            temporalBuckets.get(quid).foreach { temporalBucket =>
              reduceSingleTemporalBucket(schema, parsedTemporalAggregatedQuery.aggregation)(mathContext)(
                temporalBucket.bits).foreach { record =>
                subscribedActorsByQueryId
                  .get(quid)
                  .foreach(
                    e =>
                      e.foreach(
                        _ ! RecordsPublished(quid,
                                             parsedTemporalAggregatedQuery.db,
                                             parsedTemporalAggregatedQuery.namespace,
                                             parsedTemporalAggregatedQuery.metric,
                                             Seq(record))))

                temporalBuckets -= quid
              }

              parsedTemporalAggregatedQuery.gracePeriod.foreach { period =>
                if (temporalBucket.lowerBound >= timeContext.currentTime - period)
                  lateTemporalBuckets += (LateTemporalBucketKey(quid,
                                                                temporalBucket.lowerBound,
                                                                temporalBucket.upperBound) -> temporalBucket)
              }

            }

            lateEventsToCheck
              .getOrElse(quid, Seq.empty)
              .groupBy { lateEvent =>
                lateTemporalBuckets
                  .find {
                    case (key, _) => key.contains(lateEvent.timestamp)
                  }
              }
              .foreach {
                case (Some((key, lateBucket)), lateEvents) =>
                  lateTemporalBuckets += (key -> lateBucket.copy(bits = lateBucket.bits ++ lateEvents))
                  reduceSingleTemporalBucket(schema, parsedTemporalAggregatedQuery.aggregation)(mathContext)(
                    lateBucket.bits ++ lateEvents).foreach { record =>
                    subscribedActorsByQueryId
                      .get(quid)
                      .foreach(
                        e =>
                          e.foreach(
                            _ ! RecordsPublished(quid,
                                                 parsedTemporalAggregatedQuery.db,
                                                 parsedTemporalAggregatedQuery.namespace,
                                                 parsedTemporalAggregatedQuery.metric,
                                                 Seq(record))))
                  }
              }

            lateEventsToCheck -= quid

            temporalAggregatedQueries.get(quid).fold() { oldQuery =>
              val newCurrentTime = oldQuery.timeContext.currentTime + interval
              temporalAggregatedQueries += (quid -> new NSDbQuery(
                oldQuery.uuid,
                oldQuery.query,
                oldQuery.parsedQuery,
                oldQuery.timeContext.copy(currentTime = newCurrentTime)))

              val toBeRemoved = lateTemporalBuckets.filter {
                case (bucketKey, _) =>
                  bucketKey.upperBound < newCurrentTime - interval
              }.keys
              lateTemporalBuckets --= toBeRemoved

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
                  .foreach(e => e.foreach(_ ! RecordsPublished(quid, db, namespace, metric, Seq(record))))
              temporaryIndex.close()
            case parsedQuery: ParsedTemporalAggregatedQuery =>
              def updateLateEvents() =
                lateEventsToCheck
                  .get(quid)
                  .fold(
                    lateEventsToCheck += (quid -> Seq(record))
                  ) { previousList =>
                    lateEventsToCheck += (quid -> (previousList :+ record))
                  }

              if (coordinatesMatch(nsdbQuery, db, namespace, metric) && temporaryIndex
                    .query(schema, parsedQuery.q, Seq.empty, 1, None)
                    .lengthCompare(1) == 0)
                temporalBuckets
                  .get(quid)
                  .fold(
                    if (record.timestamp >= timeContext.currentTime)
                      temporalBuckets += (quid -> TemporalBucket(timeContext.currentTime,
                                                                 timeContext.currentTime + parsedQuery.interval,
                                                                 ListBuffer(record)))
                    else if (record.timestamp >= timeContext.currentTime - parsedQuery.gracePeriod.getOrElse(0L))
                      updateLateEvents()
                    else
                      ()
                  ) { previousBucket =>
                    if (record.timestamp >= timeContext.currentTime) {
                      temporalBuckets += (quid -> previousBucket.copy(bits = previousBucket.bits :+ record))
                    } else if (record.timestamp >= timeContext.currentTime - parsedQuery.gracePeriod.getOrElse(0L))
                      updateLateEvents()
                    else
                      ()
                  }
            case _ => // do nothing
          }
        case _ => // do nothing
      }
    case Unsubscribe(actor) =>
      log.debug("unsubscribe actor {} ", actor)
      subscribedActorsByQueryId.foreach {
        case (quid, actors) if actors.contains(actor) =>
          if (actors.size == 1) {
            subscribedActorsByQueryId -= quid
            temporalAggregatedTasks.get(quid).foreach(_.cancel())
            temporalAggregatedTasks -= quid
          } else
            subscribedActorsByQueryId += (quid -> (actors - actor))
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
  case class TemporalBucket(lowerBound: Long, upperBound: Long, bits: ListBuffer[Bit]) {
    def contains(timestamp: Long): Boolean = timestamp <= upperBound && timestamp >= lowerBound
  }

  /**
    * Defines a key for a subscribed query within a time interval.
    */
  case class LateTemporalBucketKey(quid: String, lowerBound: Long, upperBound: Long) {
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

    case class PushTemporalAggregatedQueries(quid: String, interval: Long) extends ControlMessage with NSDbSerializable
    case object PushStandardAggregatedQueries                              extends ControlMessage with NSDbSerializable
  }

  object Events {
    case class Unsubscribed(actor: ActorRef) extends ControlMessage with NSDbSerializable
  }
}
