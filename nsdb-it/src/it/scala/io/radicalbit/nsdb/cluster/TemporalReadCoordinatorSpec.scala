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
import io.radicalbit.nsdb.test.MiniClusterSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}


class TemporalReadCoordinatorSpec extends MiniClusterSpec {

  override val replicationFactor: Int = 1
  override val shardInterval: Duration = Duration.ofSeconds(30)

  val db        = "db"
  val namespace = "registry"

  override def beforeAll(): Unit = {

    super.beforeAll()

    val nsdbConnection =
      eventually {
        Await.result(NSDB.connect(host = firstNode.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      }

    TemporalLongMetric.testRecords.map(_.asApiBit(db, namespace, TemporalLongMetric.name)).foreach { bit =>
      eventually {
        assert(Await.result(nsdbConnection.write(bit), 10.seconds).errors == "")
      }
    }

    TemporalDoubleMetric.testRecords.map(_.asApiBit(db, namespace, TemporalDoubleMetric.name)).foreach { bit =>
      eventually {
        assert(Await.result(nsdbConnection.write(bit), 10.seconds).errors == "")
      }
    }

    waitIndexing()
  }

  test("execute a temporal count query on a Long metric") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalLongMetric.name} group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 5)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(30000,2L,Map("lowerBound" -> 0L, "upperBound" -> 30000L),Map()),
          Bit(60000,1L,Map("lowerBound" -> 30000L, "upperBound" -> 60000L),Map()),
          Bit(90000,1L,Map("lowerBound" -> 60000L, "upperBound" -> 90000L),Map()),
          Bit(120000,1L,Map("lowerBound" -> 90000L, "upperBound" -> 120000L),Map()),
          Bit(150000,1L,Map("lowerBound" -> 120000L, "upperBound" -> 150000L),Map())
        )
        )
      }
    }
  }

  test("execute a temporal count query when no shard is picked up") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalLongMetric.name} where timestamp > 200000 group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.isEmpty)
        assert(readRes.records.map(_.asBit) == Seq())
      }
    }
  }

  test("execute a temporal count query when only one shard is picked up") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalLongMetric.name} where timestamp > 100000 group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(120000, 1L, Map("lowerBound" -> 100001L, "upperBound" -> 120000L), Map()),
          Bit(150000, 1L, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
        ))
      }
    }
  }

  test("execute a temporal count query with a limit") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalLongMetric.name} group by interval 30s limit 2")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(120000, 1L, Map("lowerBound" -> 90000L, "upperBound" -> 120000L), Map()),
          Bit(150000, 1L, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
        ))
      }
    }
  }

  test("execute a temporal count query with a timestamp descending ordering") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalLongMetric.name} group by interval 30s order by timestamp desc")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 5)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(150000,1L,Map("lowerBound" -> 120000L, "upperBound" -> 150000L),Map()),
          Bit(120000,1L,Map("lowerBound" -> 90000L, "upperBound" -> 120000L),Map()),
          Bit(90000,1L,Map("lowerBound" -> 60000L, "upperBound" -> 90000L),Map()),
          Bit(60000,1L,Map("lowerBound" -> 30000L, "upperBound" -> 60000L),Map()),
          Bit(30000,2L,Map("lowerBound" -> 0L, "upperBound" -> 30000L),Map())
        )
        )
      }
    }
  }

  test("execute a temporal count query when time ranges contain more than one value") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalLongMetric.name} group by interval 60s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 3)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(60000, 3L, Map("lowerBound"      -> 0L, "upperBound"      -> 60000L), Map()),
          Bit(120000, 2L, Map("lowerBound"  -> 60000L, "upperBound"  -> 120000L), Map()),
          Bit(180000, 1L, Map("lowerBound"  -> 120000L, "upperBound"  -> 180000L), Map())
        ))
      }
    }
  }

  test("execute a temporal count on a double metric") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalDoubleMetric.name} group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 5)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(30000, 2L, Map("lowerBound" -> 0L, "upperBound" -> 30000L), Map()),
          Bit(60000, 1L, Map("lowerBound" -> 30000L, "upperBound" -> 60000L), Map()),
          Bit(90000, 1L, Map("lowerBound" -> 60000L, "upperBound" -> 90000L), Map()),
          Bit(120000, 1L, Map("lowerBound" -> 90000L, "upperBound" -> 120000L), Map()),
          Bit(150000, 1L, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
        ))
      }
    }
  }

  test("execute a temporal count on a double metric when time ranges contain more than one value") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalDoubleMetric.name} group by interval 60s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 3)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(60000, 3L, Map("lowerBound"      -> 0L, "upperBound"      -> 60000L), Map()),
          Bit(120000, 2L, Map("lowerBound"  -> 60000L, "upperBound"  -> 120000L), Map()),
          Bit(180000, 1L, Map("lowerBound"  -> 120000L, "upperBound"  -> 180000L), Map())
        ))
      }
    }
  }

  test("execute a temporal count with an interval higher than the shard interval")  {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalDoubleMetric.name} group by interval 100s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(80000, 3L, Map("lowerBound"     -> 0L, "upperBound"     -> 80000L), Map()),
          Bit(180000, 3L, Map("lowerBound" -> 80000L, "upperBound" -> 180000L), Map())
        ))
      }
    }
  }

  test("execute a temporal query in case of a where condition") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${TemporalDoubleMetric.name} where timestamp >= 60000 group by interval 100s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(80000, 1L, Map("lowerBound" -> 60000L, "upperBound" -> 80000L), Map()),
          Bit(180000, 3L, Map("lowerBound" -> 80000L, "upperBound" -> 180000L), Map())
        ))
      }
    }

    }

    test("execute a temporal query with sum aggregation on a double metric") {

      nodes.foreach { n =>
        val nsdb =
          Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

        val query = nsdb
          .db(db)
          .namespace(namespace)
          .query(s"select sum(*) from ${TemporalDoubleMetric.name} group by interval 30s")

        eventually {
          val readRes = Await.result(nsdb.execute(query), 10.seconds)

          assert(readRes.completedSuccessfully)
          assert(readRes.records.size == 5)
          assert(readRes.records.map(_.asBit) == Seq(
            Bit(30000, 6.0 , Map("lowerBound" -> 0L, "upperBound" -> 30000L), Map()),
            Bit(60000, 7.5, Map("lowerBound" -> 30000L, "upperBound" -> 60000L), Map()),
            Bit(90000, 5.5, Map("lowerBound" -> 60000L, "upperBound" -> 90000L), Map()),
            Bit(120000, 3.5, Map("lowerBound" -> 90000L, "upperBound" -> 120000L), Map()),
            Bit(150000, 2.5, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
          ))
        }
      }
    }

  test("execute a temporal query with max aggregation") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select max(*) from ${TemporalLongMetric.name} group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 5)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(30000, 4L , Map("lowerBound" -> 0L, "upperBound" -> 30000L), Map()),
          Bit(60000, 7L, Map("lowerBound" -> 30000L, "upperBound" -> 60000L), Map()),
          Bit(90000, 5L, Map("lowerBound" -> 60000L, "upperBound" -> 90000L), Map()),
          Bit(120000, 3L, Map("lowerBound" -> 90000L, "upperBound" -> 120000L), Map()),
          Bit(150000, 2L, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
        ))
      }
    }
  }

  test("execute a temporal query with max aggregation on a double metric") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select max(*) from ${TemporalDoubleMetric.name} group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 5)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(30000, 4.5 , Map("lowerBound" -> 0L, "upperBound" -> 30000L), Map()),
          Bit(60000, 7.5, Map("lowerBound" -> 30000L, "upperBound" -> 60000L), Map()),
          Bit(90000, 5.5, Map("lowerBound" -> 60000L, "upperBound" -> 90000L), Map()),
          Bit(120000, 3.5, Map("lowerBound" -> 90000L, "upperBound" -> 120000L), Map()),
          Bit(150000, 2.5, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
        ))
      }
    }
  }

  test("execute a temporal query with min aggregation") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select min(*) from ${TemporalLongMetric.name} group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 5)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(30000, 1L , Map("lowerBound" -> 0L, "upperBound" -> 30000L), Map()),
          Bit(60000, 7L, Map("lowerBound" -> 30000L, "upperBound" -> 60000L), Map()),
          Bit(90000, 5L, Map("lowerBound" -> 60000L, "upperBound" -> 90000L), Map()),
          Bit(120000, 3L, Map("lowerBound" -> 90000L, "upperBound" -> 120000L), Map()),
          Bit(150000, 2L, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
        ))
      }
    }
  }

  test("execute a temporal query with min aggregation on a double metric") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select min(*) from ${TemporalDoubleMetric.name} group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 5)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(30000, 1.5 , Map("lowerBound" -> 0L, "upperBound" -> 30000L), Map()),
          Bit(60000, 7.5, Map("lowerBound" -> 30000L, "upperBound" -> 60000L), Map()),
          Bit(90000, 5.5, Map("lowerBound" -> 60000L, "upperBound" -> 90000L), Map()),
          Bit(120000, 3.5, Map("lowerBound" -> 90000L, "upperBound" -> 120000L), Map()),
          Bit(150000, 2.5, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
        ))
      }
    }
  }


  test("execute a temporal query with avg aggregation") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select avg(*) from ${TemporalLongMetric.name} group by interval 50s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 4)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(0, 2.5 , Map("lowerBound" -> 0L, "upperBound" -> 30000L), Map()),
          Bit(30000, 7.0, Map("lowerBound" -> 30000L, "upperBound" -> 80000L), Map()),
          Bit(80000, 4.0, Map("lowerBound" -> 80000L, "upperBound" -> 130000L), Map()),
          Bit(130000, 2.0, Map("lowerBound" -> 130000L, "upperBound" -> 180000L), Map())
        ))
      }
    }
  }

  test("execute a temporal query with avg aggregation on a double metric") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select avg(*) from ${TemporalDoubleMetric.name} group by interval 30s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 5)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(30000, 3.0 , Map("lowerBound" -> 0L, "upperBound" -> 30000L), Map()),
          Bit(60000, 7.5, Map("lowerBound" -> 30000L, "upperBound" -> 60000L), Map()),
          Bit(90000, 5.5, Map("lowerBound" -> 60000L, "upperBound" -> 90000L), Map()),
          Bit(120000, 3.5, Map("lowerBound" -> 90000L, "upperBound" -> 120000L), Map()),
          Bit(150000, 2.5, Map("lowerBound" -> 120000L, "upperBound" -> 150000L), Map())
        ))
      }
    }
  }

  test("execute a temporal query with count distinct aggregation") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count( distinct *) from ${TemporalLongMetric.name} group by interval 50s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 3)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(0,1L,Map("lowerBound" -> 0L, "upperBound" -> 50000L),Map(),Set()),
          Bit(50000,1L,Map("lowerBound" -> 50000L, "upperBound" -> 100000L),Map(),Set()),
          Bit(100000,1L,Map("lowerBound" -> 100000L, "upperBound" -> 150000L),Map(),Set()),
        ))
      }
    }
  }

  test("execute a temporal query with count distinct aggregation on a tag") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count( distinct height) from ${TemporalLongMetric.name} group by interval 50s")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(readRes.records.map(_.asBit) == Seq(
          Bit(0,1L,Map("lowerBound" -> 0L, "upperBound" -> 30000L),Map(),Set()),
          Bit(30000,1L,Map("lowerBound" -> 30000L, "upperBound" -> 60000L),Map(),Set()),
          Bit(60000,1L,Map("lowerBound" -> 60000L, "upperBound" -> 90000L),Map(),Set()),
          Bit(90000,1L,Map("lowerBound" -> 90000L, "upperBound" -> 120000L),Map(),Set()),
          Bit(120000,1L,Map("lowerBound" -> 120000L, "upperBound" -> 150000L),Map(),Set()),
          Bit(150000,1L,Map("lowerBound" -> 150000L, "upperBound" -> 180000L),Map(),Set())
        ))
      }
    }
  }

}
