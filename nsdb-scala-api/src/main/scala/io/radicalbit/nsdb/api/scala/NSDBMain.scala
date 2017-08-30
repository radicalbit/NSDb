package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.common.protocol.SQLStatementExecuted

import scala.concurrent._
import scala.concurrent.duration._

object NSDBMain extends App {

  val res: Future[SQLStatementExecuted] = NSDB
    .connect(host = "127.0.0.1", port = 2552)
    .namespace("registry")
    .metric("people")
    .value(34)
    .dimension("surname", "pluto")
    .write()

  println(Await.result(res, 10 seconds))
}
