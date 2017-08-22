package io.radicalbit.nsdb.web

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._

trait StaticResources {
  val staticResources: Route =
    get {
      pathEnd {
        redirect("/index.html", StatusCodes.MovedPermanently)
      }
    }
}
