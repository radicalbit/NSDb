/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import akka.actor.ActorSystem
import io.radicalbit.nsdb.api.scala.{Bit, NSDB}
import io.radicalbit.nsdb.rpc.response.RPCInsertResult

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object NSDBPusher extends App {

  val system = ActorSystem("pusher")
  import system.dispatcher
  val connection = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817), 10.seconds)
  system.scheduler.scheduleAtFixedRate(0.seconds, 1.second) { () =>
    val series: Bit = connection
      .db("radicalbit")
      .namespace("channel")
      .metric("event")
      .value(new java.math.BigDecimal("13.5"))
      .dimension("city", "Milano")
      .tag("gender", "M")
      .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
      .dimension("bigDecimalDouble", new java.math.BigDecimal("12.5"))

    val res: Future[RPCInsertResult] = connection.write(series)

    println(Await.result(res, 10.seconds))

  }

}

object NSDbStreamingMain extends App {

  import io.radicalbit.nsdb.api.scala.streaming.NSDbStreaming._

  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global

  val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817), 10 seconds)

  val query = nsdb
    .db("radicalbit")
    .namespace("channel")
    .metric("event")
    .query("select * from event limit 1")

  nsdb.subscribe(query) { response =>
    println(response)
  }

  Await.result(Future.never, Duration.Inf)
}
