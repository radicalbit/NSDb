package io.radicalbit.nsdb.web

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

/**
  * Allows Cross-Origin call to exposed Http Apis.
  */
trait CorsSupport {

  val optionsSupport = {
    options { complete("") }
  }

  val corsHeaders = List(
    RawHeader("Access-Control-Allow-Origin", "*"),
    RawHeader("Access-Control-Allow-Credentials", "true"),
    RawHeader("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS, DELETE"),
    RawHeader(
      "Access-Control-Allow-Headers",
      "Access-Control-Allow-Origin, Access-Control-Allow-Credentials, X-Requested-With, Content-Type"
    )
  )

  def withCors(route: Route) = respondWithHeaders(corsHeaders) { route ~ optionsSupport }

}
