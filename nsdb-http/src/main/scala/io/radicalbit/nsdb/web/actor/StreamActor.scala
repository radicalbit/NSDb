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

package io.radicalbit.nsdb.web.actor

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events._
import io.radicalbit.nsdb.common.protocol.NSDbSerializable
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.web.actor.StreamActor._

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * Bridge actor between [[io.radicalbit.nsdb.actors.PublisherActor]] and the WebSocket channel.
  * @param clientAddress the client address that established the connection (for logging and monitoring purposes).
  * @param publisher global Publisher Actor.
  * @param publishInterval publish to web socket interval.
  * @param securityHeaderPayload payload of the security header. @see NSDBAuthProvider#headerName.
  * @param authProvider the configured [[NSDBAuthProvider]]
  */
class StreamActor(clientAddress: String,
                  publisher: ActorRef,
                  publishInterval: Int,
                  securityHeaderPayload: Option[String],
                  authProvider: NSDBAuthProvider)
    extends ActorPathLogging {

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.stream.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  import context.dispatcher

  override def receive: Receive = waiting

  private val buffer: mutable.Queue[RecordsPublished] = mutable.Queue.empty

  /**
    * Waits for the WebSocket actor reference behaviour.
    */
  def waiting: Receive = {
    case Connect(wsActor) =>
      import scala.concurrent.duration._

      context.system.scheduler.schedule(0.seconds, publishInterval.millis) {
        while (buffer.nonEmpty) wsActor ! OutgoingMessage(buffer.dequeue())
      }

      log.info("establishing web socket connection from {}", clientAddress)
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
    case msg @ (SubscribedByQueryString(_, _, _) | SubscriptionByQueryStringFailed(_, _) |
        SubscriptionByQuidFailed(_, _)) =>
      wsActor ! OutgoingMessage(msg.asInstanceOf[AnyRef])
    case msg @ RecordsPublished(_, _, _) =>
      buffer += msg
    case Terminate =>
      log.info("closing web socket connection from address {}", clientAddress)
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
  case class Connect(outgoing: ActorRef)      extends NSDbSerializable
  case class OutgoingMessage(message: AnyRef) extends NSDbSerializable

  case object Terminate
  case class RegisterQuery(db: String, namespace: String, metric: String, queryString: String)
      extends Metric
      with NSDbSerializable
  case class QuerystringRegistrationFailed(db: String,
                                           namespace: String,
                                           metric: String,
                                           queryString: String,
                                           reason: String)
      extends NSDbSerializable
  case class QuidRegistrationFailed(db: String, namespace: String, metric: String, quid: String, reason: String)
      extends NSDbSerializable

  def props(clientAddress: String,
            publisherActor: ActorRef,
            refreshPeriod: Int,
            securityHeader: Option[String],
            authProvider: NSDBAuthProvider) =
    Props(new StreamActor(clientAddress: String, publisherActor, refreshPeriod, securityHeader, authProvider))
}
