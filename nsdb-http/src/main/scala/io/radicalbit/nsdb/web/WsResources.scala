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

package io.radicalbit.nsdb.web

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.RemoteAddress
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketUpgrade}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.radicalbit.nsdb.protocol.RealTimeProtocol.Events.SubscriptionByQueryStringFailed
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.web.NSDbJson._
import io.radicalbit.nsdb.web.actor.StreamActor
import io.radicalbit.nsdb.web.actor.StreamActor._
import spray.json._

import scala.collection.JavaConverters._

trait WsResources {

  implicit def system: ActorSystem

  def logger: LoggingAdapter

  /** Publish refresh period default value , also considered as the min value */
  private val refreshPeriod = system.settings.config.getInt("nsdb.websocket.refresh-period")

  /** Number of messages that can be retained before being published,
    * if the buffered messages exceed this threshold, they will be discarded
    **/
  private val retentionSize = system.settings.config.getInt("nsdb.websocket.retention-size")

  /**
    * Akka stream Flow used to define the webSocket behaviour.
    *
    * @param clientAddress         the client address that opened the connection (for debugging and monitoring purposes).
    * @param publishInterval       interval of data publishing operation.
    * @param retentionSize         size of the buffer used to retain events in case of no subscribers.
    * @param publisherActor        the global [[io.radicalbit.nsdb.actors.PublisherActor]].
    * @param wsSubProtocols        all the subprotocols of the webSocket request.
    * @param authProvider          the configured [[NSDbAuthorizationProvider]].
    * @return the [[Flow]] that models the WebSocket.
    */
  private def newStream(clientAddress: String,
                        publishInterval: Int,
                        retentionSize: Int,
                        publisherActor: ActorRef,
                        wsSubProtocols: Seq[String],
                        authProvider: NSDbAuthorizationProvider): Flow[Message, Message, NotUsed] = {

    /**
      * Bridge actor between [[io.radicalbit.nsdb.actors.PublisherActor]] and the WebSocket channel.
      */
    val connectedWsActor = system.actorOf(
      StreamActor
        .props(clientAddress, publisherActor, refreshPeriod)
        .withDispatcher("akka.actor.control-aware-dispatcher"))

    /**
      * Messages from the Ws to the backend.
      */
    val incomingMessages: Sink[Message, NotUsed] =
      Flow[Message]
        .map {
          case TextMessage.Strict(text) =>
            text.parseJson.convertOpt[RegisterQuery] match {
              case Some(registerQuery: RegisterQuery) =>
                val securityPayload = authProvider.extractWsSecurityPayload(wsSubProtocols.asJava)
                val authorizationResult = authProvider.checkMetricAuth(registerQuery.db,
                                                                       registerQuery.namespace,
                                                                       registerQuery.metric,
                                                                       securityPayload,
                                                                       false)
                if (authorizationResult.isSuccess)
                  registerQuery
                else
                  SubscriptionByQueryStringFailed(registerQuery.db,
                                                  registerQuery.namespace,
                                                  registerQuery.metric,
                                                  registerQuery.queryString,
                                                  s"unauthorized ${authorizationResult.getFailReason}")
              case None => s"Message $text not handled by receiver"
            }
//            text.parseJson.convertOpt[RegisterQuery] getOrElse
          case _ => "Message not handled by receiver"
        }
        .to(Sink.actorRef(connectedWsActor, Terminate, _.getMessage))

    /**
      * Messages from the backend to the Ws.
      */
    val outgoingMessages: Source[Message, NotUsed] =
      Source
        .actorRef[StreamActor.OutgoingMessage](retentionSize, OverflowStrategy.dropNew)
        .mapMaterializedValue { outgoingActor =>
          connectedWsActor ! StreamActor.Connect(outgoingActor)
          NotUsed
        }
        .map {
          case OutgoingMessage(message) =>
            TextMessage(message.toJson.compactPrint)
        }

    Flow
      .fromSinkAndSource(incomingMessages, outgoingMessages)
  }

  /**
    * WebSocket route handling WebSocket requests.
    * User can optionally define data refresh period, using query parameter `refresh_period` and data retention size using query parameter `retention_size`.
    * If nor `refresh_period` or `retention_size` is defined the default one is used.
    * User defined `refresh_period` cannot be less than the default value specified in `nsdb.refresh-period`.
    *
    * @param publisherActor actor publisher of class [[io.radicalbit.nsdb.actors.PublisherActor]]
    * @param authProvider   authentication provider implementing [[NSDbAuthorizationProvider]] class
    * @return ws route
    */
  def wsResources(publisherActor: ActorRef, authProvider: NSDbAuthorizationProvider): Route =
    path("ws-stream") {
      extractClientIP { remoteAddress: RemoteAddress =>
        parameter('refresh_period ? refreshPeriod, 'retention_size ? retentionSize) {
          case (period, retention) if period >= refreshPeriod =>
            extractWebSocketUpgrade { u: WebSocketUpgrade =>
              val subProtocols = u.getRequestedProtocols().iterator().asScala.toSeq
              logger.debug("found sub protocols in ws request {}", subProtocols)

              handleWebSocketMessagesForOptionalProtocol(
                newStream(remoteAddress.toOption.map(_.getHostAddress).getOrElse("unknown"),
                          period,
                          retention,
                          publisherActor,
                          subProtocols,
                          authProvider),
                subProtocols.headOption
              )
            }
          case (period, _) =>
            complete(
              (BadRequest,
               s"publish period of $period milliseconds cannot be used, must be greater or equal to $refreshPeriod"))
        }
      }
    }
}
