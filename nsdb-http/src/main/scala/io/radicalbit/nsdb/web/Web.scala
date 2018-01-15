package io.radicalbit.nsdb.web

import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetPublisher, GetReadCoordinator, GetWriteCoordinator}
import org.json4s.DefaultFormats

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait Web extends StaticResources with WsResources with ApiResources with CorsSupport with SSLSupport {

  implicit val formats = DefaultFormats

  val config                = system.settings.config
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher   = system.dispatcher
  implicit val timeout: Timeout

  def guardian: ActorRef

  Future
    .sequence(
      Seq((guardian ? GetPublisher).mapTo[ActorRef],
          (guardian ? GetReadCoordinator).mapTo[ActorRef],
          (guardian ? GetWriteCoordinator).mapTo[ActorRef]))
    .onComplete {
      case Success(Seq(publisher, readCoordinator, writeCoordinator)) =>
        val api: Route = staticResources ~ wsResources(publisher) ~ apiResources(publisher,
                                                                                 readCoordinator,
                                                                                 writeCoordinator)

        val http =
          if (isSSLEnabled)
            Http().bindAndHandle(withCors(api),
                                 config.getString("nsdb.http.interface"),
                                 config.getInt("nsdb.http.ssl-port"),
                                 connectionContext = serverContext)
          else
            Http().bindAndHandle(withCors(api),
                                 config.getString("nsdb.http.interface"),
                                 config.getInt("nsdb.http.port"))

        scala.sys.addShutdownHook {
          http.flatMap(_.unbind()).onComplete { _ =>
            system.terminate()
          }

          Await.result(system.whenTerminated, 60 seconds)
        }
      case Failure(ex) =>
    }

}
