package io.radicalbit.nsdb.web

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Command.RemoveQuery
import io.radicalbit.nsdb.actors.PublisherActor.Events.QueryRemoved
import io.radicalbit.nsdb.core.CoreActors

import scala.concurrent.Future

trait QueryResources { this: CoreActors =>

  implicit def system: ActorSystem

  implicit val disp = system.dispatcher

  implicit val timeout: Timeout

  private def deleteQuery(id: String, publisherActor: ActorRef): Future[String] = {
    (publisherActor ? RemoveQuery(id)).mapTo[QueryRemoved].map(_.quid)
  }

  def queryResources(publisherActor: ActorRef): Route =
    pathPrefix("query") {
      pathPrefix(JavaUUID) { id =>
        pathEnd {
          delete {
            complete(deleteQuery(id.toString, publisherActor))
          }
        }
      }
    }

}
