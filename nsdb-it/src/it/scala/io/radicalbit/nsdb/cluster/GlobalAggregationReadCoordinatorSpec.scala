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

import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.client.rpc.converter.GrpcBitConverters._
import io.radicalbit.nsdb.cluster.testdata.TestMetrics._
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter
import io.radicalbit.nsdb.statement.StatementParserErrors.NO_GROUP_BY_AGGREGATION
import io.radicalbit.nsdb.test.MiniClusterSpec

import java.time.Duration
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class GlobalAggregationReadCoordinatorSpec extends MiniClusterSpec {

  override val replicationFactor: Int  = 1
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

    AggregationLongMetric.testRecords.map(_.asApiBit(db, namespace, AggregationLongMetric.name)).foreach { bit =>
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

  test("receive a select containing a global count distinct ") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(distinct *) from ${AggregationLongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.reason == "")
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("count(distinct *)" -> 4L))))
      }
    }
  }

  test("receive a select containing a global count distinct on a tag") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(distinct height) from ${AggregationLongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.reason == "")
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("count(distinct *)" -> 3L))))
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

  test("receive a select containing a global max") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select max(*) from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("max(*)" -> 6L))))
      }
    }
  }

  test("receive a select containing a global min") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select min(*) from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("min(*)" -> 1L))))
      }
    }
  }

  test("receive a select containing a global sum") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select sum(*) from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("sum(*)" -> 21L))))
      }
    }
  }

  test("receive a select containing mixed global aggregations") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select max(*), min(*), sum(*), avg(*), count(*), count(distinct *) from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(readRes.records.map(_.asBit) == Seq(Bit(0, 0L, Map.empty, Map("max(*)" -> 6L, "min(*)" -> 1L, "sum(*)" -> 21L, "avg(*)" -> 3.5, "count(distinct *)" -> 6L, "count(*)" -> 6L))))
      }
    }
  }

  test("receive a select containing mixed aggregations and plain fields") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(
          s"select count(*), count(distinct *), avg(*), max(*), min(*), sum(*), name from ${AggregationLongMetric.name} order by timestamp")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(
          readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
            Bit(1L,
              2L,
              Map(),
              Map("name"              -> "John",
                "count(*)"          -> 6L,
                "count(distinct *)" -> 4L,
                "avg(*)"            -> 2.166666667,
                "max(*)"            -> 4L,
                "min(*)"            -> 1L,
                "sum(*)"            -> 13L)),
            Bit(2L,
              3L,
              Map(),
              Map("name"              -> "John",
                "count(*)"          -> 6L,
                "count(distinct *)" -> 4L,
                "avg(*)"            -> 2.166666667,
                "max(*)"            -> 4L,
                "min(*)"            -> 1L,
                "sum(*)"            -> 13L)),
            Bit(4L,
              4L,
              Map(),
              Map("name"              -> "John",
                "count(*)"          -> 6L,
                "count(distinct *)" -> 4L,
                "avg(*)"            -> 2.166666667,
                "max(*)"            -> 4L,
                "min(*)"            -> 1L,
                "sum(*)"            -> 13L)),
            Bit(6L,
              1L,
              Map(),
              Map("name"              -> "Bill",
                "count(*)"          -> 6L,
                "count(distinct *)" -> 4L,
                "avg(*)"            -> 2.166666667,
                "max(*)"            -> 4L,
                "min(*)"            -> 1L,
                "sum(*)"            -> 13L)),
            Bit(8L,
              1L,
              Map(),
              Map("name"              -> "Frank",
                "count(*)"          -> 6L,
                "count(distinct *)" -> 4L,
                "avg(*)"            -> 2.166666667,
                "max(*)"            -> 4L,
                "min(*)"            -> 1L,
                "sum(*)"            -> 13L)),
            Bit(10L,
              2L,
              Map(),
              Map("name"              -> "Frankie",
                "count(*)"          -> 6L,
                "count(distinct *)" -> 4L,
                "avg(*)"            -> 2.166666667,
                "max(*)"            -> 4L,
                "min(*)"            -> 1L,
                "sum(*)"            -> 13L))
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
        assert(
          readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
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

  test("receive a select containing mixed max(*) and plain fields") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select max(*), name from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(
          readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
            Bit(1L, 1L, Map.empty, Map("name"  -> "John", "max(*)"    -> 6L)),
            Bit(2L, 2L, Map.empty, Map("name"  -> "John", "max(*)"    -> 6L)),
            Bit(4L, 3L, Map.empty, Map("name"  -> "J", "max(*)"       -> 6L)),
            Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "max(*)"    -> 6L)),
            Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "max(*)"   -> 6L)),
            Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "max(*)" -> 6L))
          ))
      }
    }
  }

  test("receive a select containing mixed min(*) and plain fields") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select min(*), name from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(
          readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
            Bit(1L, 1L, Map.empty, Map("name"  -> "John", "min(*)"    -> 1L)),
            Bit(2L, 2L, Map.empty, Map("name"  -> "John", "min(*)"    -> 1L)),
            Bit(4L, 3L, Map.empty, Map("name"  -> "J", "min(*)"       -> 1L)),
            Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "min(*)"    -> 1L)),
            Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "min(*)"   -> 1L)),
            Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "min(*)" -> 1L))
          ))
      }
    }
  }

  test("receive a select containing mixed sum(*) and plain fields") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select sum(*), name from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
          Bit(1L, 1L, Map.empty, Map("name"  -> "John", "sum(*)"    -> 21L)),
          Bit(2L, 2L, Map.empty, Map("name"  -> "John", "sum(*)"    -> 21L)),
          Bit(4L, 3L, Map.empty, Map("name"  -> "J", "sum(*)"       -> 21L)),
          Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "sum(*)"    -> 21L)),
          Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "sum(*)"   -> 21L)),
          Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "sum(*)" -> 21L))
        ))
      }
    }
  }

  test("receive a select containing mixed aggregations with where condition") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*), min(*), max(*), sum(*) from ${AggregationLongMetric.name} where height < 31")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(
          readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
            Bit(0L, 0L, Map.empty, Map("min(*)" -> 2L, "max(*)" -> 4L, "sum(*)" -> 9L, "count(*)" -> 3L))
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
        .query(s"select first(*) from ${LongMetric.name}")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(!readRes.completedSuccessfully)
        assert(readRes.reason == NO_GROUP_BY_AGGREGATION)
      }
    }
  }
}
