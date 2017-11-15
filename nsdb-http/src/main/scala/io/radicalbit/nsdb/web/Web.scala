package io.radicalbit.nsdb.web

import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetPublisher, GetReadCoordinator}
import org.json4s.DefaultFormats

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait Web extends StaticResources with WsResources with QueryResources with CorsSupport {

  implicit val formats = DefaultFormats

  val config                = system.settings.config
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher   = system.dispatcher
  implicit val timeout: Timeout

  def guardian: ActorRef

  Future
    .sequence(Seq((guardian ? GetPublisher).mapTo[ActorRef], (guardian ? GetReadCoordinator).mapTo[ActorRef]))
    .onComplete {
      case Success(Seq(publisher, readCoordinator)) =>
        val api: Route = staticResources ~ wsResources(publisher) ~ queryResources(publisher, readCoordinator)

        val http =
          Http().bindAndHandle(withCors(api), config.getString("nsdb.http.interface"), config.getInt("nsdb.http.port"))

        scala.sys.addShutdownHook {
          http.flatMap(_.unbind()).onComplete { _ =>
            system.terminate()
          }

          Await.result(system.whenTerminated, 60 seconds)
        }
      case Failure(ex) =>
    }

}
