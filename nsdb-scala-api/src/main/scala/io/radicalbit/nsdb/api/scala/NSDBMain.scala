package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.rpc.response.RPCInsertResult

import scala.concurrent._
import scala.concurrent.duration._

object NSDBMain extends App {

  val nsdb = NSDB.connect(host = "127.0.0.1", port = 2552)(ExecutionContext.global)

  val series = nsdb
    .namespace("registry")
    .series("people")
    .bit
    .metric("age", 18)
    .field("name", "Goofy")
    .field("address", "Goofy Road")
    .field("best_friend", "Mickey Mouse")
    .dimension("city", "Mouseton")
    .dimension("gender", "M")

  val res: Future[RPCInsertResult] = nsdb.write(series)

  println(Await.result(res, 10 seconds))
}
