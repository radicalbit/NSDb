package io.radicalbit.nsdb.web

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.OverflowStrategy
import akka.stream.contrib.TimeWindow
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.util.Config
import io.radicalbit.nsdb.web.actor.StreamActor
import io.radicalbit.nsdb.web.actor.StreamActor._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

import scala.concurrent.duration.FiniteDuration

trait WsResources {

  implicit val formats: DefaultFormats.type

  implicit def system: ActorSystem

  private val publishInterval = system.settings.config.getInt("nsdb.publish-interval")

  private def newStream(publishInterval: Int,
                        publisherActor: ActorRef,
                        header: Option[String],
                        authProvider: NSDBAuthProvider): Flow[Message, Message, NotUsed] = {

    val connectedWsActor = system.actorOf(StreamActor.props(publisherActor, header, authProvider))

    val incomingMessages: Sink[Message, NotUsed] =
      Flow[Message]
        .map {
          case TextMessage.Strict(text) =>
            parse(text).extractOpt[RegisterQuery] orElse
              parse(text).extractOpt[RegisterQuid] getOrElse "Message not handled by receiver"
          case _ => "Message not handled by receiver"
        }
        .to(Sink.actorRef(connectedWsActor, Terminate))

    val outgoingMessages: Source[Message, NotUsed] =
      Source
        .actorRef[StreamActor.OutgoingMessage](10, OverflowStrategy.dropTail)
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
      .via(
        TimeWindow(FiniteDuration(publishInterval, TimeUnit.MILLISECONDS), eager = true)(identity[Message])(
          (_, newMessage) => newMessage))

  }

  def wsResources(publisherActor: ActorRef, authProvider: NSDBAuthProvider): Route =
    path("ws-stream") {
      parameter('interval ? publishInterval) { interval =>
        optionalHeaderValueByName(authProvider.headerName) { header =>
          handleWebSocketMessages(newStream(interval, publisherActor, header, authProvider))
        }
      }
    }
}
