package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import io.radicalbit.nsdb.actors.PublisherActor.Command._
import io.radicalbit.nsdb.actors.PublisherActor.Events._
import io.radicalbit.nsdb.common.protocol.BitOut
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.coordinator.ReadCoordinator.{ExecuteStatement, SelectStatementExecuted}
import io.radicalbit.nsdb.coordinator.WriteCoordinator
import io.radicalbit.nsdb.index.{NsdbQuery, QueryIndex, TemporaryIndex}
import io.radicalbit.nsdb.statement.StatementParser
import org.apache.lucene.store.FSDirectory
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.concurrent.duration._

class PublisherActor(val basePath: String, readCoordinator: ActorRef) extends Actor with ActorLogging {

  lazy val queryIndex: QueryIndex = new QueryIndex(FSDirectory.open(Paths.get(basePath, "queries")))

  lazy val subscribedActors: mutable.Map[String, Set[ActorRef]] = mutable.Map.empty

  lazy val queries: mutable.Map[String, NsdbQuery] = mutable.Map.empty

  override def preStart(): Unit = {
    queries ++= queryIndex.getAll.map(s => s.uuid -> s).toMap
  }

  implicit val disp = context.system.dispatcher

  override def receive = {
    case SubscribeBySqlStatement(actor, query) =>
      subscribedActors
        .find { case (_, v) => v == actor }
        .fold {
          new StatementParser().parseStatement(query) match {
            case Success(_) =>
              val id = queries.find { case (k, v) => v.query == query }.map(_._1) getOrElse
                UUID.randomUUID().toString
              val previousRegisteredActors = subscribedActors.getOrElse(id, Set.empty)
              subscribedActors += (id -> (previousRegisteredActors + actor))
              queries += (id          -> NsdbQuery(id, query))

              implicit val timeout = Timeout(3 seconds)

              (readCoordinator ? ExecuteStatement(query))
                .mapTo[SelectStatementExecuted[BitOut]]
                .map(e => Subscribed(id, e.values))
                .pipeTo(sender())

              implicit val writer = queryIndex.getWriter
              queryIndex.write(NsdbQuery(id, query))
              writer.close()
            case Failure(ex) => sender ! SubscriptionFailed(ex.getMessage)
          }
        } {
          case (id, _) =>
            implicit val timeout = Timeout(3 seconds)
            (readCoordinator ? ExecuteStatement(query))
              .mapTo[SelectStatementExecuted[BitOut]]
              .map(e => Subscribed(id, e.values))
              .pipeTo(sender())
        }
    case SubscribeByQueryId(actor, quid) =>
      queries.get(quid) match {
        case Some(q) =>
          val previousRegisteredActors = subscribedActors.getOrElse(quid, Set.empty)
          subscribedActors += (quid -> (previousRegisteredActors + actor))
          implicit val timeout = Timeout(3 seconds)
          (readCoordinator ? ExecuteStatement(q.query))
            .mapTo[SelectStatementExecuted[BitOut]]
            .map(e => Subscribed(quid, e.values))
            .pipeTo(sender())
        case None => sender ! SubscriptionFailed(s"quid $quid not found")
      }
    case WriteCoordinator.InputMapped(_, metric, record) =>
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
                subscribedActors(id).foreach(_ ! RecordPublished(id, metric, BitOut(record)))
            case Failure(query) =>
              log.error(s"query ${nsdbQuery.query} not valid")
          }
      }
    case Unsubscribe(actor) =>
      subscribedActors.find { case (_, v) => v.contains(actor) }.foreach {
        case (k, v) =>
          subscribedActors += (k -> (v - actor))
          sender() ! Unsubscribed
      }
    case RemoveQuery(quid) =>
      subscribedActors.get(quid).foreach { actors =>
        actors.foreach(_ ! PoisonPill)
      }
      subscribedActors -= quid
      queries -= quid
      sender() ! QueryRemoved(quid)
  }
}

object PublisherActor {

  def props(basePath: String, readCoordinator: ActorRef): Props = Props(new PublisherActor(basePath, readCoordinator))

  object Command {
    case class SubscribeBySqlStatement(actor: ActorRef, query: SelectSQLStatement)
    case class SubscribeByQueryId(actor: ActorRef, qid: String)
    case class Unsubscribe(actor: ActorRef)
    case class RemoveQuery(quid: String)
  }

  object Events {
    case class Subscribed(quid: String, records: Seq[BitOut])
    case class SubscriptionFailed(reason: String)

    case class RecordPublished(quid: String, metric: String, record: BitOut)
    case object Unsubscribed
    case class QueryRemoved(quid: String)
  }
}
