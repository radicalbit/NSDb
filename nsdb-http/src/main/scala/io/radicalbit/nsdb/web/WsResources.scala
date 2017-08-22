package io.radicalbit.nsdb.web

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.radicalbit.nsdb.web.actor.StreamActor
import io.radicalbit.nsdb.web.actor.StreamActor.{OutgoingMessage, RegisterQuery, RegisterQuid, Terminate}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write

trait WsResources {

  implicit val formats = DefaultFormats

  implicit def system: ActorSystem

  private def newStream(publisherActor: ActorRef): Flow[Message, Message, NotUsed] = {

    val connectedWsActor = system.actorOf(StreamActor.props(publisherActor))

    val incomingMessages: Sink[Message, NotUsed] =
      Flow[Message]
        .map {
          case TextMessage.Strict(text) =>
            parse(text).extractOpt[RegisterQuery] orElse
              parse(text).extractOpt[RegisterQuid] getOrElse "ERROR"
          //handle errors
        }
        .to(Sink.actorRef(connectedWsActor, Terminate))

    val outgoingMessages: Source[Message, NotUsed] =
      Source
        .actorRef[StreamActor.OutgoingMessage](10, OverflowStrategy.fail)
        .mapMaterializedValue { outgoingActor =>
          connectedWsActor ! StreamActor.Connect(outgoingActor)
          NotUsed
        }
        .map {
          case OutgoingMessage(message) =>
            TextMessage(write(message))
        }

    Flow.fromSinkAndSource(incomingMessages, outgoingMessages)
  }

  def wsResources(publisherActor: ActorRef): Route =
    path("ws-stream") {
      handleWebSocketMessages(newStream(publisherActor))
    }
}
