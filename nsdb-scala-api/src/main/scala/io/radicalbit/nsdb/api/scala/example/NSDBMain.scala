/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.api.scala.example

import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse

import scala.concurrent._
import scala.concurrent.duration._

/**
  * Example App for writing a Bit.
  */
object NSDBMainWrite extends App {

  val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

  val series = nsdb
    .db("root")
    .namespace("registry")
    .bit("people")
    .value(Some(new java.math.BigDecimal("13")))
    .dimension("city", "Mouseton")
    .dimension("notimportant", None)
    .dimension("Someimportant", Some(2))
    .dimension("gender", "M")
    .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
    .dimension("bigDecimalDouble", new java.math.BigDecimal("12.5"))
    .dimension("OptionBigDecimal", Some(new java.math.BigDecimal("15.5")))

  val res: Future[RPCInsertResult] = nsdb.write(series)

  println(Await.result(res, 10.seconds))
}

/**
  * Example App for executing a query.
  */
object NSDBMainRead extends App {

  val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

  val query = nsdb
    .db("root")
    .namespace("registry")
    .query("select * from people limit 1")

  val readRes: Future[SQLStatementResponse] = nsdb.execute(query)

  println(Await.result(readRes, 10.seconds))
}
