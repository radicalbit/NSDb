package io.radicalbit.nsdb.web

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.radicalbit.nsdb.web.actor.StreamActor
import io.radicalbit.nsdb.web.actor.StreamActor._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

trait WsResources {

  implicit val formats: DefaultFormats.type

  implicit def system: ActorSystem

  private def newStream(publisherActor: ActorRef): Flow[Message, Message, NotUsed] = {

    val connectedWsActor = system.actorOf(StreamActor.props(publisherActor))

    val incomingMessages: Sink[Message, NotUsed] =
      Flow[Message]
        .map {
          case TextMessage.Strict(text) =>
            parse(text).extractOpt[RegisterQuery] orElse
              parse(text).extractOpt[RegisterQuid] orElse
              parse(text).extractOpt[RegisterQuids] orElse
              parse(text).extractOpt[RegisterQueries] getOrElse "Message not handled by receiver"
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

    Flow.fromSinkAndSource(incomingMessages, outgoingMessages)
  }

  def wsResources(publisherActor: ActorRef): Route =
    path("ws-stream") {
      handleWebSocketMessages(newStream(publisherActor))
    }
}
