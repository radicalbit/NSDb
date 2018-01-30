package io.radicalbit.nsdb.api.scala

import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse

import scala.concurrent._
import scala.concurrent.duration._

object NSDBMainWrite extends App {

  val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

  val series = nsdb
    .db("root")
    .namespace("registry")
    .bit("people")
    .value(10)
    .dimension("city", "Mouseton")
    .dimension("notimportant", None)
    .dimension("Someimportant", Some(2))
    .dimension("gender", "M")

  val res: Future[RPCInsertResult] = nsdb.write(series)

  println(Await.result(res, 10.seconds))
}

object NSDBMainRead extends App {

  val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

  val query = nsdb
    .db("root")
    .namespace("registry")
    .query("select * from people limit 1")

  val readRes: Future[SQLStatementResponse] = nsdb.execute(query)

  println(Await.result(readRes, 10.seconds))
}
