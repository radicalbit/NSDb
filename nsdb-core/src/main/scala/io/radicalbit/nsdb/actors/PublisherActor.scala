package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeByQueryId, SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events.{RecordPublished, Subscribed, SubscriptionFailed, Unsubscribed}
import io.radicalbit.nsdb.common.protocol.Record
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.index.{NsdbQuery, QueryIndex, TemporaryIndex}
import io.radicalbit.nsdb.statement.StatementParser
import org.apache.lucene.store.FSDirectory

import scala.collection.mutable
import scala.util.{Failure, Success}

class PublisherActor(val basePath: String) extends Actor with ActorLogging {

  lazy val queryIndex: QueryIndex = new QueryIndex(FSDirectory.open(Paths.get(basePath, "queries")))

  lazy val subscribedActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  lazy val queries: mutable.Map[String, NsdbQuery] = mutable.Map.empty

  override def preStart(): Unit = {
    queries ++= queryIndex.getAll.map(s => s.uuid -> s).toMap
  }

  override def receive = {
    case SubscribeBySqlStatement(actor, query) =>
      subscribedActors
        .find { case (_, v) => v == actor }
        .fold {
          new StatementParser().parseStatement(query) match {
            case Success(qr) =>
              val id = queries.find { case (k, v) => v.query == query }.map(_._1) getOrElse
                UUID.randomUUID().toString
              subscribedActors += (id -> actor)
              queries += (id          -> NsdbQuery(id, query))
              sender ! Subscribed(id)
              implicit val writer = queryIndex.getWriter
              queryIndex.write(NsdbQuery(id, query))
              writer.close()
            case Failure(ex) => sender ! SubscriptionFailed(ex.getMessage)
          }
        } {
          case (id, _) => sender() ! Subscribed(id)
        }
    case SubscribeByQueryId(actor, quid) =>
      queries.get(quid) match {
        case Some(q) =>
          subscribedActors -= quid
          subscribedActors += (quid -> actor)
          sender() ! Subscribed(quid)
        case None => sender ! SubscriptionFailed(s"quid $quid not found")
      }
    case msg @ RecordPublished(metric, record) =>
      val temporaryIndex: TemporaryIndex = new TemporaryIndex()
      implicit val writer                = temporaryIndex.getWriter
      temporaryIndex.write(record)
      writer.close()
      queries.foreach {
        case (id, nsdbQuery) =>
          val luceneQuery = new StatementParser().parseStatement(nsdbQuery.query)
          luceneQuery match {
            case Success(parsedQuery) =>
              if (metric == nsdbQuery.query.metric && temporaryIndex.query(parsedQuery.q, 1, None).size == 1)
                subscribedActors(id) ! msg
            case Failure(query) =>
              log.error(s"query ${nsdbQuery.query} not valid")
          }

      }
    case Unsubscribe(actor) => {
      subscribedActors.find { case (_, v) => v == actor }.foreach {
        case (k, _) =>
          subscribedActors -= k
          sender() ! Unsubscribed
      }
    }
  }
}

object PublisherActor {

  def props(basePath: String): Props = Props(new PublisherActor(basePath))

  object Command {
    case class SubscribeBySqlStatement(actor: ActorRef, query: SelectSQLStatement)
    case class SubscribeByQueryId(actor: ActorRef, qid: String)
    case class Unsubscribe(actor: ActorRef)
  }

  object Events {
    case class Subscribed(qid: String)
    case class SubscriptionFailed(reason: String)

    case class RecordPublished(metric: String, record: Record)
    case object Unsubscribed
  }
}
