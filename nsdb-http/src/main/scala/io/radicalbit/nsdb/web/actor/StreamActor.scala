package io.radicalbit.nsdb.web.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeByQueryId, SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events._
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.web.actor.StreamActor._

import scala.concurrent.Future
import scala.util.{Failure, Success}

class StreamActor(publisher: ActorRef, securityHeader: Option[String], authProvider: NSDBAuthProvider)
    extends Actor
    with ActorLogging {

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.stream.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  import context.dispatcher

  override def receive: Receive = waiting

  def waiting: Receive = {
    case Connect(wsActor) =>
      context become connected(wsActor)
  }

  def connected(wsActor: ActorRef): Receive = {
    case msg @ RegisterQuery(db, namespace, metric, queryString) =>
      val checkAuthorization =
        authProvider.checkMetricAuth(ent = msg, header = securityHeader getOrElse "", writePermission = false)
      if (checkAuthorization.success)
        new SQLStatementParser().parse(db, namespace, queryString) match {
          case Success(statement) if statement.isInstanceOf[SelectSQLStatement] =>
            (publisher ? SubscribeBySqlStatement(self, queryString, statement.asInstanceOf[SelectSQLStatement]))
              .map {
                case msg @ SubscribedByQueryString(_, _, _) =>
                  OutgoingMessage(msg)
                case SubscriptionFailed(reason) =>
                  OutgoingMessage(QuerystringRegistrationFailed(db, namespace, metric, queryString, reason))
              }
              .pipeTo(wsActor)
          case Success(_) =>
            wsActor ! OutgoingMessage(
              QuerystringRegistrationFailed(db, namespace, metric, queryString, "not a select query"))
          case Failure(ex) =>
            wsActor ! OutgoingMessage(QuerystringRegistrationFailed(db, namespace, metric, queryString, ex.getMessage))
        } else
        wsActor ! OutgoingMessage(
          QuerystringRegistrationFailed(db,
                                        namespace,
                                        metric,
                                        queryString,
                                        s"unauthorized ${checkAuthorization.failReason}"))
    case msg @ RegisterQuid(db, namespace, metric, quid) =>
      log.debug(s"registering quid $quid")
      val checkAuthorization =
        authProvider.checkMetricAuth(ent = msg, header = securityHeader getOrElse "", writePermission = false)
      val result =
        if (checkAuthorization.success)
          (publisher ? SubscribeByQueryId(self, quid))
            .map {
              case msg @ SubscribedByQuid(_, _) =>
                OutgoingMessage(msg)
              case SubscriptionFailed(reason) =>
                OutgoingMessage(QuidRegistrationFailed(db, namespace, metric, quid, reason))
            } else
          Future(QuidRegistrationFailed(db, namespace, metric, quid, s"unauthorized ${checkAuthorization.failReason}"))
      result.pipeTo(wsActor)
    case msg @ RecordsPublished(_, _, _) =>
      wsActor ! OutgoingMessage(msg)
    case Terminate =>
      log.debug("terminating stream actor")
      (publisher ? Unsubscribe(self)).foreach { _ =>
        self ! PoisonPill
        wsActor ! PoisonPill
      }
    case msg @ _ =>
      log.error(s"Unexpected message in ws : $msg")
      wsActor ! OutgoingMessage("invalid message sent")
  }
}

object StreamActor {
  case class Connect(outgoing: ActorRef)
  case class OutgoingMessage(message: AnyRef)

  case object Terminate
  case class RegisterQuery(db: String, namespace: String, metric: String, queryString: String) extends Metric
  case class RegisterQuid(db: String, namespace: String, metric: String, quid: String)         extends Metric
  case class QuerystringRegistrationFailed(db: String,
                                           namespace: String,
                                           metric: String,
                                           queryString: String,
                                           reason: String)
  case class QuidRegistrationFailed(db: String, namespace: String, metric: String, quid: String, reason: String)

  def props(publisherActor: ActorRef, securityHeader: Option[String], authProvider: NSDBAuthProvider) =
    Props(new StreamActor(publisherActor, securityHeader, authProvider))
}
