package io.radicalbit.nsdb.cluster

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
import io.radicalbit.nsdb.client.rpc.converter.GrpcBitConverters.GrpcBitConverter
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.minicluster.converters.BitConverters.ApiBitConverter
import io.radicalbit.nsdb.test.MiniClusterSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class WriteCoordinatorClusterSpec extends MiniClusterSpec {

  override val nodesNumber: Int = 3

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  test("join cluster") {
    eventually {
      assert(
        Cluster(minicluster.nodes.head.system).state.members
          .count(_.status == MemberStatus.Up) == minicluster.nodes.size)
    }
  }

  test("add record from first node") {

    val firstNode = minicluster.nodes.head

    val timestamp = System.currentTimeMillis()

    val nsdb =
      Await.result(NSDB.connect(host = "127.0.0.1", port = firstNode.grpcPort)(ExecutionContext.global), 10.seconds)

    val bit = nsdb
      .db("root")
      .namespace("registry")
      .metric("people")
      .timestamp(timestamp)
      .value(new java.math.BigDecimal("13"))
      .dimension("city", "Mouseton")
      .dimension("notimportant", None)
      .dimension("Someimportant", Some(2))
      .tag("gender", "M")
      .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
      .dimension("bigDecimalDouble", new java.math.BigDecimal("12.5"))
      .dimension("OptionBigDecimal", Some(new java.math.BigDecimal("15.5")))

    eventually {
      val res = Await.result(nsdb.write(bit), 10.seconds)
      assert(res.completedSuccessfully)
    }

    waitIndexing()

    val query = nsdb
      .db("root")
      .namespace("registry")
      .query("select * from people limit 1")

    eventually {
      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.completedSuccessfully)
      assert(readRes.records.size == 1)
      assert(readRes.records.head.asBit == bit.asCommonBit)
    }
  }

  test("add record from last node") {

    val secondNode = minicluster.nodes.last

    val nsdb =
      eventually {
        Await.result(NSDB.connect(host = "127.0.0.1", port = secondNode.grpcPort)(ExecutionContext.global), 10.seconds)
      }

    val bit = Bit(timestamp = System.currentTimeMillis(),
                  dimensions = Map("city" -> "Mouseton", "gender" -> "F"),
                  value = 2,
                  tags = Map.empty)

    import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter

    val apiBit = bit.asApiBit("root", "registry", "people")

    eventually {
      val res = Await.result(nsdb.write(apiBit), 10.seconds)
      assert(res.completedSuccessfully)
    }

    waitIndexing()

    val query = nsdb
      .db("root")
      .namespace("registry")
      .query("select * from people limit 2")

    eventually {
      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.completedSuccessfully)
      assert(readRes.records.size == 2)
    }
  }
}
