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

package io.radicalbit.nsdb.cluster

import java.time.Duration

import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.client.rpc.converter.GrpcBitConverters._
import io.radicalbit.nsdb.cluster.testdata.TestMetrics._
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter
import io.radicalbit.nsdb.statement.StatementParserErrors.NO_GROUP_BY_AGGREGATION
import io.radicalbit.nsdb.test.MiniClusterSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}


class GlobalAggregationReadCoordinatorSpec extends MiniClusterSpec {

  override val replicationFactor: Int = 1
  override val shardInterval: Duration = Duration.ofSeconds(30)

  val db        = "db"
  val namespace = "registry"

  override def beforeAll(): Unit = {

    super.beforeAll()

    val firstNode = nodes.head

    val nsdbConnection =
      eventually {
        Await.result(NSDB.connect(host = firstNode.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      }

    LongMetric.testRecords.map(_.asApiBit(db, namespace, LongMetric.name)).foreach { bit =>
      eventually {
        assert(Await.result(nsdbConnection.write(bit), 10.seconds).completedSuccessfully)
      }
    }

  }

  test("receive a select containing a global count") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("count(*)" -> 6L))))
      }
    }
  }

  test("receive a select containing a global count with a limit") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${LongMetric.name} limit 4")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("count(*)" -> 4L))))
      }
    }
  }

  test("receive a select containing a global average") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select avg(*) from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("avg(*)" -> 3.5))))
      }
    }
  }

  test("receive a select containing mixed count and plain fields") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*), name from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
          Bit(1L, 1L, Map.empty, Map("name"  -> "John", "count(*)"    -> 6L)),
          Bit(2L, 2L, Map.empty, Map("name"  -> "John", "count(*)"    -> 6L)),
          Bit(4L, 3L, Map.empty, Map("name"  -> "J", "count(*)"       -> 6L)),
          Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "count(*)"    -> 6L)),
          Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "count(*)"   -> 6L)),
          Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "count(*)" -> 6L))
        ))
      }
    }
  }

  test("receive a select containing mixed average and plain fields") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select avg(*), name from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
          Bit(1L, 1L, Map.empty, Map("name"  -> "John", "avg(*)"    -> 3.5)),
          Bit(2L, 2L, Map.empty, Map("name"  -> "John", "avg(*)"    -> 3.5)),
          Bit(4L, 3L, Map.empty, Map("name"  -> "J", "avg(*)"       -> 3.5)),
          Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "avg(*)"    -> 3.5)),
          Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "avg(*)"   -> 3.5)),
          Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "avg(*)" -> 3.5))
        ))
      }
    }
  }

  test("receive a select containing mixed count, average and plain fields") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*), avg(*), name from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
          Bit(1L, 1L, Map.empty, Map("name"  -> "John", "avg(*)"    -> 3.5, "count(*)" -> 6L)),
          Bit(2L, 2L, Map.empty, Map("name"  -> "John", "avg(*)"    -> 3.5, "count(*)" -> 6L)),
          Bit(4L, 3L, Map.empty, Map("name"  -> "J", "avg(*)"       -> 3.5, "count(*)" -> 6L)),
          Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "avg(*)"    -> 3.5, "count(*)" -> 6L)),
          Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "avg(*)"   -> 3.5, "count(*)" -> 6L)),
          Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "avg(*)" -> 3.5, "count(*)" -> 6L))
        ))
      }
    }
  }

  test("receive a select containing non global aggregation without a group by") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select sum(*) from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(!readRes.completedSuccessfully)
        assert(readRes.reason == NO_GROUP_BY_AGGREGATION)
      }
    }
  }
}
