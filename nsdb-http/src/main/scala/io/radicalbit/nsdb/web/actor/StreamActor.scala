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
import io.radicalbit.nsdb.actors.PublisherActor.Commands._
import io.radicalbit.nsdb.actors.RealTimeProtocol.Events._
import io.radicalbit.nsdb.actors.RealTimeProtocol.RealTimeOutGoingMessage
import io.radicalbit.nsdb.common.NSDbLongType
import io.radicalbit.nsdb.common.protocol.NSDbSerializable
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.monitoring.NSDbMonitoring
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import io.radicalbit.nsdb.util.ActorPathLogging
import io.radicalbit.nsdb.web.Filters.Filter
import io.radicalbit.nsdb.web.QueryEnriched
import io.radicalbit.nsdb.web.actor.StreamActor._
import kamon.Kamon

import scala.collection.mutable

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

  lazy val kamonMetric = Kamon.gauge(NSDbMonitoring.NSDbWsConnectionsTotal).withoutTags()

  override def preStart(): Unit = {
    kamonMetric.increment()
  }

  override def postStop(): Unit = {
    kamonMetric.decrement()
  }

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
    case msg @ RegisterQuery(db, namespace, metric, inputQueryString, from, to, filtersOpt) =>
      val checkAuthorization =
        authProvider.checkMetricAuth(ent = msg, header = securityHeaderPayload getOrElse "", writePermission = false)
      if (checkAuthorization.success)
        QueryEnriched(db,
                      namespace,
                      inputQueryString,
                      from.map(_.rawValue),
                      to.map(_.rawValue),
                      filtersOpt.getOrElse(Seq.empty)) match {
          case SqlStatementParserSuccess(queryString, statement: SelectSQLStatement) =>
            publisher ! SubscribeBySqlStatement(self, db, namespace, metric, queryString, statement)
          case SqlStatementParserSuccess(queryString, _) =>
            wsActor ! OutgoingMessage(
              SubscriptionByQueryStringFailed(db, namespace, metric, queryString, "not a select statement"))
          case SqlStatementParserFailure(queryString, error) =>
            wsActor ! OutgoingMessage(SubscriptionByQueryStringFailed(db, namespace, metric, queryString, error))
        } else
        wsActor ! OutgoingMessage(
          SubscriptionByQueryStringFailed(db,
                                          namespace,
                                          metric,
                                          inputQueryString,
                                          s"unauthorized ${checkAuthorization.failReason}"))
    case msg: SubscribedByQueryString =>
      wsActor ! OutgoingMessage(msg)
    case msg: SubscriptionByQueryStringFailed => wsActor ! OutgoingMessage(msg)
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
      wsActor ! OutgoingMessage(ErrorResponse("invalid message sent"))
  }
}

object StreamActor {
  case class Connect(outgoing: ActorRef)                       extends NSDbSerializable
  case class OutgoingMessage(message: RealTimeOutGoingMessage) extends NSDbSerializable

  case object Terminate
  case class RegisterQuery(db: String,
                           namespace: String,
                           metric: String,
                           queryString: String,
                           from: Option[NSDbLongType] = None,
                           to: Option[NSDbLongType] = None,
                           filters: Option[Seq[Filter]] = None)
      extends Metric
      with NSDbSerializable

  def props(clientAddress: String,
            publisherActor: ActorRef,
            refreshPeriod: Int,
            securityHeader: Option[String],
            authProvider: NSDBAuthProvider) =
    Props(new StreamActor(clientAddress: String, publisherActor, refreshPeriod, securityHeader, authProvider))
}
