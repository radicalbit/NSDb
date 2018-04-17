package io.radicalbit.nsdb.web

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._

trait StaticResources {
  val staticResources: Route =
    get {
      getFromDirectory("nsdb-web-ui/build/index.html")
    } ~ getFromDirectory("nsdb-web-ui/build/")
}
