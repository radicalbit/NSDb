/*
 * Copyright 2018 Radicalbit S.r.l.
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
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.web.actor.StreamActor
import io.radicalbit.nsdb.web.actor.StreamActor._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

trait WsResources {

  implicit val formats: DefaultFormats.type

  implicit def system: ActorSystem

  /** Publish refresh period default value , also considered as the min value */
  private val refreshPeriod = system.settings.config.getInt("nsdb.websocket.refresh-period")

  /** Number of messages that can be retained before being published,
    * if the buffered messages exceed this threshold, they will be discarded
    **/
  private val retentionSize = system.settings.config.getInt("nsdb.websocket.retention-size")

  /**
    * Akka stream Flow used to define the webSocket behaviour.
    * @param publishInterval interval of data publishing operation.
    * @param retentionSize size of the buffer used to retain events in case of no subscribers.
    * @param publisherActor the global [[io.radicalbit.nsdb.actors.PublisherActor]].
    * @param securityHeaderPayload payload of the security header. @see NSDBAuthProvider#headerName.
    * @param authProvider the configured [[NSDBAuthProvider]].
    * @return the [[Flow]] that models the WebSocket.
    */
  private def newStream(publishInterval: Int,
                        retentionSize: Int,
                        publisherActor: ActorRef,
                        securityHeaderPayload: Option[String],
                        authProvider: NSDBAuthProvider): Flow[Message, Message, NotUsed] = {

    /**
      * Bridge actor between [[io.radicalbit.nsdb.actors.PublisherActor]] and the WebSocket channel.
      */
    val connectedWsActor = system.actorOf(
      StreamActor
        .props(publisherActor, refreshPeriod, securityHeaderPayload, authProvider)
        .withDispatcher("akka.actor.control-aware-dispatcher"))

    /**
      * Messages from the Ws to the backend.
      */
    val incomingMessages: Sink[Message, NotUsed] =
      Flow[Message]
        .map {
          case TextMessage.Strict(text) =>
            parse(text).extractOpt[RegisterQuery] orElse
              parse(text).extractOpt[RegisterQuid] getOrElse "Message not handled by receiver"
          case _ => "Message not handled by receiver"
        }
        .to(Sink.actorRef(connectedWsActor, Terminate))

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
            TextMessage(write(message))
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
    * @param authProvider authentication provider implementing [[NSDBAuthProvider]] class
    * @return ws route
    */
  def wsResources(publisherActor: ActorRef, authProvider: NSDBAuthProvider): Route =
    path("ws-stream") {
      parameter('refresh_period ? refreshPeriod, 'retention_size ? retentionSize) {
        case (period, retention) if period >= refreshPeriod =>
          optionalHeaderValueByName(authProvider.headerName) { header =>
            handleWebSocketMessages(newStream(period, retention, publisherActor, header, authProvider))
          }
        case (period, _) =>
          complete(
            (BadRequest,
             s"publish period of $period milliseconds cannot be used, must be greater or equal to $refreshPeriod"))
      }
    }
}
