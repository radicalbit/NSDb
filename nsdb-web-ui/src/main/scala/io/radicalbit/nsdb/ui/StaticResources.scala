package io.radicalbit.nsdb.ui

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._

trait StaticResources {
  val staticResources: Route =
    (get & pathPrefix("ui")) {
      (pathEndOrSingleSlash & redirectToTrailingSlashIfMissing(StatusCodes.TemporaryRedirect)) {
        getFromResource("ui/index.html")
      } ~ getFromResourceDirectory(".")
    }

}
