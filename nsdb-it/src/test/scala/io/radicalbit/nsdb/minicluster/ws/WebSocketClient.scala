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

package io.radicalbit.nsdb.minicluster.ws

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

/**
  * Utility class that creates a WebSocket with convenience methods to subscribe to a NSDb query
  * @param host the host to connect to
  * @param port the port to connect to
  */
class WebSocketClient(host: String, port: Int) extends LazyLogging with SynchronizedBuffer[Message] {
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()
  import system.dispatcher
  val req           = WebSocketRequest(uri = s"ws://$host:$port/ws-stream")
  val webSocketFlow = Http().webSocketClientFlow(req)

  val messageSource: Source[Message, ActorRef] =
    Source.actorRef[TextMessage.Strict](bufferSize = 10, OverflowStrategy.fail)

  val messageSink: Sink[Message, NotUsed] =
    Flow[Message]
      .map { message =>
        logger.debug(s"Received text message: [$message]")
        accumulate(message)
      }
      .to(Sink.ignore)

  val ((ws, upgradeResponse), closed) =
    messageSource
      .viaMat(webSocketFlow)(Keep.both)
      .toMat(messageSink)(Keep.both)
      .run()

  val connected = upgradeResponse.flatMap { upgrade =>
    if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
      Future.successful(Done)
    } else {
      throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
    }
  }

  def send(msg: String): Unit = {
    ws ! TextMessage.Strict(msg)
  }

  def receivedBuffer(): ListBuffer[Message] = buffer

  def subscribe(db: String, namespace: String, metric: String): Unit =
    ws ! TextMessage.Strict(
      s"""{"db":"$db","namespace":"$namespace","metric":"$metric","queryString":"select * from $metric limit 1"}""")
}
