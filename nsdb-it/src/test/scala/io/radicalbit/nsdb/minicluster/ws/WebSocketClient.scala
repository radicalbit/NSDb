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

  def receivedBuffer(clear: Boolean = true): ListBuffer[Message] = {
    Thread.sleep(1000)

    val buf = buffer
    if (clear) clearBuffer()
    buf
  }

  def subscribe(db: String, namespace: String, metric: String): Unit =
    ws ! TextMessage.Strict(
      s"""{"db":"$db","namespace":"$namespace","metric":"$metric","queryString":"select * from $metric limit 1"}""")
}
