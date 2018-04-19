//package io.radicalbit.nsdb.web
//
//import akka.http.scaladsl.server.Directives._
//import akka.http.scaladsl.server._
//
//trait StaticResources {
//  val staticResources: Route =
//    get {
//      getFromResource("build/index.html")
//    } ~ getFromResourceDirectory("build")
//}
