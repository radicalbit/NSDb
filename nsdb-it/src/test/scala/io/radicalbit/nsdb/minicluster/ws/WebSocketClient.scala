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
        logger.info(s"Received text message: [$message]")
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

    val buf = getBuffer()
    if (clear) clearBuffer()
    buf
  }

  def subscribe(db: String, namespace: String, metric: String): Unit =
    ws ! TextMessage.Strict(
      s"""{"db":"$db","namespace":"$namespace","metric":"$metric","queryString":"select * from $metric limit 1"}""")
}

object WebSocketClient extends App {

  val x = new WebSocketClient("localhost", 9010)

  x.subscribe("db", "namespace", "metirc")
}
