package io.radicalbit.nsdb.web

//import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
//import akka.http.scaladsl.server.Directives._
//import akka.http.scaladsl.server._
//
//trait StaticResources {
//  val staticResources: Route =
//    get {
//      path("") {
//        redirect("/index.html", StatusCodes.MovedPermanently)
//      }
//      path("hello") {
//        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
//      }
//    }
//}
