package io.radicalbit.nsdb.actors

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.dispatch.ControlMessage
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Command._
import io.radicalbit.nsdb.actors.PublisherActor.Events._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.index.TemporaryIndex
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.statement.StatementParser.ParsedSimpleQuery
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.IndexSearcher

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Models queries used for the  subscription process.
  * @param uuid a generated, unique identifier for a query.
  * @param query the parsed select statement.
  */
case class NsdbQuery(uuid: String, query: SelectSQLStatement) {
  def aggregated: Boolean = query.groupBy.isDefined
}

/**
  * Actor responsible to accept subscriptions and publish events to its subscribers.
  * A subscription can be achieved by sending:
  *
  * - [[SubscribeBySqlStatement]] the subscription is asked providing a query string, which is parsed into [[SelectSQLStatement]]. If the query does not exist, a new one will be created and registered.
  *
  * - [[SubscribeByQueryId]] the subscription is asked providing a query id, which have already been registered.
  *
  * A generic publisher must send a [[PublishRecord]] message in order to trigger the publishing mechanism.
  * Every published event will be checked against every registered query. If the event fulfills the query, it will be published to every query relatedsubscriber.
  * @param readCoordinator global read coordinator responsible to execute queries when needed
  */
class PublisherActor(readCoordinator: ActorRef) extends Actor with ActorLogging {

  /**
    * mutable subscriber map aggregated by query id
    */
  lazy val subscribedActorsByQueryId: mutable.Map[String, Set[ActorRef]] = mutable.Map.empty

  /**
    * mutable map of queries aggregated by query id
    */
  lazy val queries: mutable.Map[String, NsdbQuery] = mutable.Map.empty

  implicit val disp: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.publisher.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  /**
    * scheduler that updates aggregated queries subscribers
    */
  context.system.scheduler.schedule(interval, interval) {
    queries
      .filter {
        case (id, q) =>
          q.aggregated && subscribedActorsByQueryId.get(id).isDefined && subscribedActorsByQueryId(id).nonEmpty
      }
      .foreach {
        case (id, nsdbQuery) =>
          val f = (readCoordinator ? ExecuteStatement(nsdbQuery.query))
            .map {
              case e: SelectStatementExecuted  => RecordsPublished(id, e.metric, e.values)
              case SelectStatementFailed(_, _) => RecordsPublished(id, nsdbQuery.query.metric, Seq.empty)
            }
          subscribedActorsByQueryId.get(id).foreach(e => e.foreach(f.pipeTo(_)))
      }
  }

  override def receive: Receive = {
    case SubscribeBySqlStatement(actor, queryString, query) =>
      subscribedActorsByQueryId
        .find { case (_, v) => v == actor }
        .fold {
          log.debug(s"subscribing a new actor to query $queryString")
          val id = queries.find { case (_, v) => v.query == query }.map(_._1) getOrElse
            UUID.randomUUID().toString

          (readCoordinator ? ExecuteStatement(query))
            .map {
              case e: SelectStatementExecuted =>
                val previousRegisteredActors = subscribedActorsByQueryId.getOrElse(id, Set.empty)
                subscribedActorsByQueryId += (id -> (previousRegisteredActors + actor))
                queries += (id                   -> NsdbQuery(id, query))
                SubscribedByQueryString(queryString, id, e.values)
              case SelectStatementFailed(reason, _) => SubscriptionFailed(reason)
            }
            .pipeTo(sender())
        } {
          case (id, _) =>
            log.debug(s"subscribing existing actor to query $queryString")
            (readCoordinator ? ExecuteStatement(query))
              .mapTo[SelectStatementExecuted]
              .map(e => SubscribedByQueryString(queryString, id, e.values))
              .pipeTo(sender())
        }
    case SubscribeByQueryId(actor, quid) =>
      queries.get(quid) match {
        case Some(q) =>
          log.debug(s"found query $q for id $quid")
          (readCoordinator ? ExecuteStatement(q.query))
            .map {
              case e: SelectStatementExecuted =>
                val previousRegisteredActors = subscribedActorsByQueryId.getOrElse(quid, Set.empty)
                subscribedActorsByQueryId += (quid -> (previousRegisteredActors + actor))
                SubscribedByQuid(quid, e.values)
              case SelectStatementFailed(reason, _) => SubscriptionFailed(reason)
            }
            .pipeTo(sender())
        case None => sender ! SubscriptionFailed(s"quid $quid not found")
      }
    case PublishRecord(db, namespace, metric, record, schema) =>
      queries.foreach {
        case (id, nsdbQuery)
            if !nsdbQuery.aggregated && nsdbQuery.query.metric == metric && subscribedActorsByQueryId
              .get(id)
              .isDefined =>
          val luceneQuery = new StatementParser().parseStatement(nsdbQuery.query, schema)
          luceneQuery match {
            case Success(parsedQuery: ParsedSimpleQuery) =>
              val temporaryIndex: TemporaryIndex = new TemporaryIndex()
              implicit val writer: IndexWriter   = temporaryIndex.getWriter
              temporaryIndex.write(record)
              writer.close()
              implicit val searcher: IndexSearcher = temporaryIndex.getSearcher
              if (db == nsdbQuery.query.db && namespace == nsdbQuery.query.namespace && metric == nsdbQuery.query.metric && temporaryIndex
                    .query(parsedQuery.q, parsedQuery.fields, 1, None)
                    .lengthCompare(1) == 0)
                subscribedActorsByQueryId
                  .get(id)
                  .foreach(e => e.foreach(_ ! RecordsPublished(id, metric, Seq(record))))
            case Success(_) => log.error("unreachable branch reached...")
            case Failure(ex) =>
              log.error(ex, s"query ${nsdbQuery.query} against schema $schema not valid because of")
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

  object Command {
    case class SubscribeBySqlStatement(actor: ActorRef, queryString: String, query: SelectSQLStatement)
        extends ControlMessage
    case class SubscribeByQueryId(actor: ActorRef, qid: String) extends ControlMessage
    case class Unsubscribe(actor: ActorRef)                     extends ControlMessage
  }

  object Events {
    case class SubscribedByQuid(quid: String, records: Seq[Bit])                             extends ControlMessage
    case class SubscribedByQueryString(queryString: String, quid: String, records: Seq[Bit]) extends ControlMessage
    case class SubscriptionFailed(reason: String)                                            extends ControlMessage

    case class RecordsPublished(quid: String, metric: String, records: Seq[Bit])
    case class Unsubscribed(actor: ActorRef)
  }
}
