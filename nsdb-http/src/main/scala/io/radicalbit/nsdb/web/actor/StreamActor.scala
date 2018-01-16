package io.radicalbit.nsdb.web.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeByQueryId, SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events._
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.web.actor.StreamActor._

import scala.concurrent.Future
import scala.util.{Failure, Success}

class StreamActor(publisher: ActorRef) extends Actor with ActorLogging {

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.stream.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  import context.dispatcher

  override def receive: Receive = waiting

  def waiting: Receive = {
    case Connect(wsActor) =>
      context become connected(wsActor)
  }

  def connected(wsActor: ActorRef): Receive = {
    case RegisterQuery(db, namespace, queryString) =>
      new SQLStatementParser().parse(db, namespace, queryString) match {
        case Success(statement) if statement.isInstanceOf[SelectSQLStatement] =>
          (publisher ? SubscribeBySqlStatement(self, queryString, statement.asInstanceOf[SelectSQLStatement]))
            .map {
              case msg @ SubscribedByQueryString(_, _, _) =>
                OutgoingMessage(msg)
              case SubscriptionFailed(reason) =>
                OutgoingMessage(QuerystringRegistrationFailed(db, namespace, queryString, reason))
            }
            .pipeTo(wsActor)
        case Success(_) =>
          wsActor ! OutgoingMessage(QuerystringRegistrationFailed(db, namespace, queryString, "not a select query"))
        case Failure(ex) =>
          wsActor ! OutgoingMessage(QuerystringRegistrationFailed(db, namespace, queryString, ex.getMessage))
      }
    case RegisterQueries(queries: Seq[RegisterQuery]) =>
      val results = queries.map(q => {
        new SQLStatementParser().parse(q.db, q.namespace, q.queryString) match {
          case Success(statement) if statement.isInstanceOf[SelectSQLStatement] =>
            (publisher ? SubscribeBySqlStatement(self, q.queryString, statement.asInstanceOf[SelectSQLStatement]))
              .map {
                case msg @ SubscribedByQueryString(_, _, _) =>
                  msg
                case SubscriptionFailed(reason) =>
                  QuerystringRegistrationFailed(q.db, q.namespace, q.queryString, reason)
              }
          case Success(_) =>
            Future(QuerystringRegistrationFailed(q.db, q.namespace, q.queryString, "not a select query"))
          case Failure(ex) =>
            Future(QuerystringRegistrationFailed(q.db, q.namespace, q.queryString, ex.getMessage))
        }
      })
      Future.sequence(results).map(OutgoingMessage).pipeTo(wsActor)
    case RegisterQuid(quid) =>
      log.debug(s"registering quid $quid")
      (publisher ? SubscribeByQueryId(self, quid))
        .map {
          case msg @ SubscribedByQuid(_, _) =>
            OutgoingMessage(msg)
          case SubscriptionFailed(reason) =>
            OutgoingMessage(QuidRegistrationFailed(quid, reason))
        }
        .pipeTo(wsActor)
    case RegisterQuids(quids) =>
      val results = quids.map(quid => {
        (publisher ? SubscribeByQueryId(self, quid))
          .map {
            case msg @ SubscribeByQueryId(_, _) =>
              msg
            case SubscriptionFailed(reason) =>
              QuidRegistrationFailed(quid, reason)
          }
      })
      Future.sequence(results).map(OutgoingMessage).pipeTo(wsActor)
    case msg @ RecordsPublished(_, _, _) =>
      wsActor ! OutgoingMessage(msg)
    case Terminate =>
      log.debug("terminating stream actor")
      (publisher ? Unsubscribe).foreach { _ =>
        self ! PoisonPill
        wsActor ! PoisonPill
      }
    case e =>
      println(e)
      wsActor ! OutgoingMessage("invalid message sent")
  }
}

object StreamActor {
  case class Connect(outgoing: ActorRef)
  case class OutgoingMessage(message: AnyRef)

  case object Terminate
  case class RegisterQuery(db: String, namespace: String, queryString: String)
  case class RegisterQueries(queries: Seq[RegisterQuery])
  case class RegisterQuid(quid: String)
  case class RegisterQuids(quids: Seq[String])
  case class QuerystringRegistrationFailed(db: String, namespace: String, queryString: String, reason: String)
  case class QuidRegistrationFailed(quid: String, reason: String)

  def props(publisherActor: ActorRef) = Props(new StreamActor(publisherActor))
}
