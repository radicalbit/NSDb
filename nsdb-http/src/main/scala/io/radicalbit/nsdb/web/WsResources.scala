package io.radicalbit.nsdb.web

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.OverflowStrategy
import akka.stream.contrib.TimeWindow
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.web.actor.StreamActor
import io.radicalbit.nsdb.web.actor.StreamActor._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

import scala.concurrent.duration.FiniteDuration

trait WsResources {

  implicit val formats: DefaultFormats.type

  implicit def system: ActorSystem

  /** Publish refresh period default value , also considered as the min value */
  private val refreshPeriod = system.settings.config.getInt("nsdb.websocket.refresh-period")

  /** Number of messages that can be retained before being published,
    * if the buffered messages exceed this threshold, they will be discarded
    **/
  private val retentionSize = system.settings.config.getInt("nsdb.websocket.retention-size")

  private def newStream(publishInterval: Int,
                        retentionSize: Int,
                        publisherActor: ActorRef,
                        header: Option[String],
                        authProvider: NSDBAuthProvider): Flow[Message, Message, NotUsed] = {

    val connectedWsActor = system.actorOf(
      StreamActor.props(publisherActor, header, authProvider).withDispatcher("akka.actor.control-aware-dispatcher"))

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
      .via(
        TimeWindow(FiniteDuration(publishInterval, TimeUnit.MILLISECONDS), eager = true)(identity[Message])(
          (_, newMessage) => newMessage))

  }

  /**
    * WebSocket route handling WebSocket requests.
    * User can optionally define data refresh period, using query parameter `refresh_period`.
    * If no `refresh_period` is defined the default one is used.
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
