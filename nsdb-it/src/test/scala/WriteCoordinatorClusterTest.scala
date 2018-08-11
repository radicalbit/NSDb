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

import java.util.concurrent.TimeUnit

import akka.cluster.{Cluster, MemberStatus}
import akka.util.Timeout
import io.radicalbit.nsdb.api.scala.NSDB

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class WriteCoordinatorClusterTest extends MiniclusterSpec {

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  test("join cluster") {
    assert(Cluster(minicluster.nodes.head.system).state.members.count(_.status == MemberStatus.Up) == 2)
  }

  test("add record from first node") {
    val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

    val bit = nsdb
      .db("root")
      .namespace("registry")
      .bit("people")
      .value(new java.math.BigDecimal("13"))
      .dimension("city", "Mouseton")
      .dimension("notimportant", None)
      .dimension("Someimportant", Some(2))
      .dimension("gender", "M")
      .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
      .dimension("bigDecimalDouble", new java.math.BigDecimal("12.5"))
      .dimension("OptionBigDecimal", Some(new java.math.BigDecimal("15.5")))

    val res = Await.result(nsdb.write(bit), 10.seconds)

    assert(res.completedSuccessfully)

    waitIndexing

    val query = nsdb
      .db("root")
      .namespace("registry")
      .query("select * from people limit 1")

    val readRes = Await.result(nsdb.execute(query), 10.seconds)

    assert(readRes.completedSuccessfully)
    assert(readRes.records.size == 1)
  }

  test("add record from second node") {
    val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7818)(ExecutionContext.global), 10.seconds)

    val bit = nsdb
      .db("root")
      .namespace("registry")
      .bit("people")
      .value(new java.math.BigDecimal("14"))
      .dimension("city", "Mouseton")
      .dimension("notimportant", None)
      .dimension("Someimportant", Some(2))
      .dimension("gender", "F")
      .dimension("bigDecimalLong", new java.math.BigDecimal("13"))
      .dimension("bigDecimalDouble", new java.math.BigDecimal("13.5"))
      .dimension("OptionBigDecimal", Some(new java.math.BigDecimal("16.5")))

    val res = Await.result(nsdb.write(bit), 10.seconds)

    assert(res.completedSuccessfully)

    waitIndexing

    val query = nsdb
      .db("root")
      .namespace("registry")
      .query("select * from people limit 2")

    val readRes = Await.result(nsdb.execute(query), 10.seconds)

    assert(readRes.completedSuccessfully)
    assert(readRes.records.size == 2)
  }
}
