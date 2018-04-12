package io.radicalbit.nsdb.web.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeByQueryId, SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events._
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.web.actor.StreamActor._

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * Bridge actor between [[io.radicalbit.nsdb.actors.PublisherActor]] and the WebSocket channel.
  * @param publisher global Publisher Actor.
  * @param publishInterval publish to web socket interval.
  * @param securityHeaderPayload payload of the security header. @see NSDBAuthProvider#headerName.
  * @param authProvider the configured [[NSDBAuthProvider]]
  */
class StreamActor(publisher: ActorRef,
                  publishInterval: Int,
                  securityHeaderPayload: Option[String],
                  authProvider: NSDBAuthProvider)
    extends Actor
    with ActorLogging {

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.stream.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  import context.dispatcher

  override def receive: Receive = waiting

  private val buffer: mutable.Map[String, RecordsPublished] = mutable.Map.empty

  /**
    * Waits for the WebSocket actor reference behaviour.
    */
  def waiting: Receive = {
    case Connect(wsActor) =>

      import scala.concurrent.duration._

      context.system.scheduler.schedule(0.seconds, publishInterval.millis) {
        val keys = buffer.keys
        keys.foreach { k =>
          wsActor ! OutgoingMessage(buffer(k))
          buffer -= k
        }
      }

      context become connected(wsActor)
  }

  /**
    * Handles registration commands and publishing events.
    * @param wsActor WebSocket actor reference.
    */
  def connected(wsActor: ActorRef): Receive = {
    case msg @ RegisterQuery(db, namespace, metric, queryString) =>
      val checkAuthorization =
        authProvider.checkMetricAuth(ent = msg, header = securityHeaderPayload getOrElse "", writePermission = false)
      if (checkAuthorization.success)
        new SQLStatementParser().parse(db, namespace, queryString) match {
          case Success(statement) if statement.isInstanceOf[SelectSQLStatement] =>
            publisher ! SubscribeBySqlStatement(self, queryString, statement.asInstanceOf[SelectSQLStatement])
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
        authProvider.checkMetricAuth(ent = msg, header = securityHeaderPayload getOrElse "", writePermission = false)
      if (checkAuthorization.success)
        publisher ! SubscribeByQueryId(self, quid)
      else
        wsActor ! OutgoingMessage(
          QuidRegistrationFailed(db, namespace, metric, quid, s"unauthorized ${checkAuthorization.failReason}"))
    case msg @ (SubscribedByQueryString(_, _, _) | SubscribedByQuid(_, _) | SubscriptionByQueryStringFailed(_, _) |
        SubscriptionByQuidFailed(_, _)) =>
      wsActor ! OutgoingMessage(msg.asInstanceOf[AnyRef])
    case msg @ RecordsPublished(quid, _, _) =>
      buffer += (quid -> msg)
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

  def props(publisherActor: ActorRef,
            refreshPeriod: Int,
            securityHeader: Option[String],
            authProvider: NSDBAuthProvider) =
    Props(new StreamActor(publisherActor, refreshPeriod, securityHeader, authProvider))
}
