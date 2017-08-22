package io.radicalbit.nsdb.web.actor

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeByQueryId, SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events.{RecordPublished, Subscribed, SubscriptionFailed}
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.web.actor.StreamActor._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class StreamActor(publisher: ActorRef) extends Actor with ActorLogging {

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  override def receive: Receive = waiting

  def waiting: Receive = {
    case Connect(wsActor) =>
      context become connected(wsActor)
  }

  def connected(wsActor: ActorRef): Receive = {
    case RegisterQuery(namespace, queryString) =>
      new SQLStatementParser().parse(namespace, queryString) match {
        case Success(statement) if statement.isInstanceOf[SelectSQLStatement] =>
          (publisher ? SubscribeBySqlStatement(self, statement.asInstanceOf[SelectSQLStatement]))
            .map {
              case msg @ Subscribed(_) =>
                context become subscribed(wsActor)
                OutgoingMessage(msg)
              case SubscriptionFailed(reason) =>
                OutgoingMessage(QuerystringRegistrationFailed(namespace, queryString, reason))
            }
            .pipeTo(wsActor)
        case Success(_) =>
          wsActor ! OutgoingMessage(QuerystringRegistrationFailed(namespace, queryString, "not a select query"))
        case Failure(ex) =>
          wsActor ! OutgoingMessage(QuerystringRegistrationFailed(namespace, queryString, ex.getMessage))
      }
    case RegisterQuid(quid) =>
      (publisher ? SubscribeByQueryId(self, quid))
        .map {
          case msg @ Subscribed(_) =>
            context become subscribed(wsActor)
            OutgoingMessage(msg)
          case SubscriptionFailed(reason) =>
            OutgoingMessage(QuidRegistrationFailed(quid, reason))
        }
        .pipeTo(wsActor)
    case _ => wsActor ! OutgoingMessage("invalid message sent")
  }

  def subscribed(wsActor: ActorRef): Receive = {
    case RegisterQuery(_, _) =>
      wsActor ! OutgoingMessage("already registered")
    case msg @ RecordPublished(_, _) =>
      wsActor ! OutgoingMessage(msg)
    case Terminate =>
      log.debug("terminating stream actor")
      (publisher ? Unsubscribe).foreach(_ => self ! PoisonPill)
    case _ => wsActor ! OutgoingMessage("invalid message sent")
  }
}

object StreamActor {
  case class Connect(outgoing: ActorRef)
  case class OutgoingMessage(message: AnyRef)

  case object Terminate
  case class RegisterQuery(namespace: String, queryString: String)
  case class RegisterQuid(quid: String)
  case class QuerystringRegistrationFailed(namespace: String, queryString: String, reason: String)
  case class QuidRegistrationFailed(quid: String, reason: String)

  def props(publisherActor: ActorRef) = Props(new StreamActor(publisherActor))
}
