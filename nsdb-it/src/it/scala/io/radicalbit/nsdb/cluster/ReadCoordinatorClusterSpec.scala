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
import io.radicalbit.nsdb.rpc.GrpcBitConverters._
import io.radicalbit.nsdb.cluster.testdata.TestMetrics._
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter
import io.radicalbit.nsdb.test.MiniClusterSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class ReadCoordinatorClusterSpec extends MiniClusterSpec {

  override val replicationFactor: Int = 1

  val db        = "db"
  val namespace = "registry"

  override def beforeAll(): Unit = {

    super.beforeAll()

    LongMetric.testRecords.map(_.asApiBit(db, namespace, LongMetric.name)).foreach { bit =>
      eventually {
        assert(Await.result(withRandomNodeConnection{_.write(bit)}, 10.seconds).completedSuccessfully)
      }
    }

    DoubleMetric.testRecords.map(_.asApiBit(db, namespace, DoubleMetric.name)).foreach { bit =>
      eventually {
        assert(Await.result(withRandomNodeConnection{_.write(bit)}, 10.seconds).completedSuccessfully)
      }
    }

    AggregationLongMetric.testRecords.map(_.asApiBit(db, namespace, AggregationLongMetric.name)).foreach { bit =>
      eventually {
        assert(Await.result(withRandomNodeConnection{_.write(bit)}, 10.seconds).completedSuccessfully)
      }
    }

    AggregationDoubleMetric.testRecords.map(_.asApiBit(db, namespace, AggregationDoubleMetric.name)).foreach { bit =>
      eventually {
        assert(Await.result(withRandomNodeConnection{_.write(bit)}, 10.seconds).completedSuccessfully)
      }
    }

    waitIndexing()
    waitIndexing()
  }

  test("receive a select projecting a wildcard with a limit") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(LongMetric.name)
        .query(s"select * from ${LongMetric.name} limit 2")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
      }
    }
  }

  test("receive a select projecting a wildcard with a limit and a ordering when ordered by timestamp") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(LongMetric.name)
        .query(s"select * from ${LongMetric.name} order by timestamp desc limit 2")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(readRes.records.map(_.asBit) == LongMetric.testRecords.reverse.take(2))
      }
    }
  }

  test(
    "receive a select projecting a wildcard with a limit and a ordering when ordered by timestamp and where condition") {

    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(LongMetric.name)
        .query(s"select * from ${LongMetric.name} where name = 'John' order by timestamp limit 2")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)

        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(readRes.records.map(_.asBit) == LongMetric.testRecords.take(2))
      }
    }
  }

  test("execute it successfully when ordered by value") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(LongMetric.name)
        .query(s"select * from ${LongMetric.name} order by value desc limit 2")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.completedSuccessfully)
      assert(readRes.records.size == 2)

      val results = readRes.records.map(_.asBit)
      results.foreach(r => assert(LongMetric.testRecords.takeRight(2).contains(r)))

    }
  }

  test("receive a select projecting a wildcard with a limit and a ordering when ordered by another dimension") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(LongMetric.name)
        .query(s"select * from ${LongMetric.name} order by name desc limit 3")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.completedSuccessfully)
      assert(readRes.records.size == 3)

      val results = readRes.records.map(_.asBit)
      results.foreach(r => assert(LongMetric.testRecords.take(3).contains(r)))
    }
  }

  test("receive a select over tags fields") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(LongMetric.name)
        .query(s"select name from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      val names = readRes.records.flatMap(_.tags.values.map(_.getStringValue))

      assert(names.count(_ == "Bill") == 1)
      assert(names.count(_ == "Frank") == 1)
      assert(names.count(_ == "Frankie") == 1)
      assert(names.count(_ == "J") == 1)
      assert(names.count(_ == "John") == 2)
    }
  }

  test("receive a select over dimensions fields") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select surname from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 6)

      val names = readRes.records.flatMap(_.dimensions.values.map(_.getStringValue))
      assert(names.count(_ == "Doe") == 5)
      assert(names.count(_ == "D") == 1)
    }
  }

  test("receive a select over dimensions and tags fields") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select name, surname from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.size == 6)

      val dimensions = readRes.records.flatMap(_.dimensions.values.map(_.getStringValue))
      assert(dimensions.count(_ == "Doe") == 5)

      val tags = readRes.records.flatMap(_.tags.values.map(_.getStringValue))

      assert(tags.count(_ == "Bill") == 1)
      assert(tags.count(_ == "Frank") == 1)
      assert(tags.count(_ == "Frankie") == 1)
      assert(tags.count(_ == "J") == 1)
      assert(tags.count(_ == "John") == 2)
    }
  }

  test("receive a select distinct over a single field") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select distinct name from ${LongMetric.name} limit 6")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      val names = readRes.records.flatMap(_.tags.values.map(_.getStringValue))

      assert(names.contains("Bill"))
      assert(names.contains("Frank"))
      assert(names.contains("Frankie"))
      assert(names.contains("John"))
      assert(names.contains("J"))
      assert(names.size == 5)
    }
  }

  test("execute successfully with limit over distinct values") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select distinct name from ${LongMetric.name} limit 2")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 2)
    }
  }

  test("execute successfully with ascending order") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select distinct name from ${LongMetric.name} order by name limit 6")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.size == 5)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 0L, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 0L, Map.empty, Map("name" -> "J")),
          Bit(0L, 0L, Map.empty, Map("name" -> "John"))
        ))
    }
  }

  test("execute successfully with descending order") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select distinct name from ${LongMetric.name} order by name desc limit 6")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.size == 5)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 0L, Map.empty, Map("name" -> "John")),
          Bit(0L, 0L, Map.empty, Map("name" -> "J")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Bill"))
        ))
    }
  }

  test("fail if receive a  distinct select projecting a wildcard") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select distinct * from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  //the more i think about this feature, the more it seems unnecessary and misleading
  ignore("receive a select projecting a list of fields with mixed aggregated and simple") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select count(*), name from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit).sortBy(_.timestamp) == Seq(
          Bit(1L, 1, Map.empty, Map("name"  -> "John", "count(*)"    -> 6)),
          Bit(2L, 1, Map.empty, Map("name"  -> "John", "count(*)"    -> 6)),
          Bit(4L, 1, Map.empty, Map("name"  -> "J", "count(*)"       -> 6)),
          Bit(6L, 1, Map.empty, Map("name"  -> "Bill", "count(*)"    -> 6)),
          Bit(8L, 1, Map.empty, Map("name"  -> "Frank", "count(*)"   -> 6)),
          Bit(10L, 1, Map.empty, Map("name" -> "Frankie", "count(*)" -> 6))
        ))
    }
  }

  test("receive a select projecting a list of fields with only a count") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select count(*) from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.size == 1)

      assert(readRes.records.head.asBit == Bit(0,0L,Map.empty, Map("count(*)" -> 6L)))
    }
  }

  test("fail if receive a select projecting a list of fields when other aggregation than count is provided") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select count(*), surname, sum(creationDate) from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  test("fail if receive a select distinct projecting a list of fields ") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select distinct name, surname from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  test("receive a select containing a range selection") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select name from ${LongMetric.name} where timestamp in (2,4) limit 4")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.size == 2)
    }
  }

  test("receive a select containing a GTE selection") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select name from ${LongMetric.name} where timestamp >= 10")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 1)
      assert(readRes.records.head.asBit == Bit(10, 6L, Map.empty, Map("name" -> "Frankie")))
    }
  }

  test("receive a select containing a GTE and a NOT selection") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select name from ${LongMetric.name} where not timestamp >= 10")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 5)
    }
  }

  test("receive a select containing a GT AND a LTE selection") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select name from ${LongMetric.name} where timestamp > 2 and timestamp <= 4 limit 4")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 1)
    }
  }

  test("receive a select containing a GTE OR a LT selection") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select name from ${LongMetric.name} where timestamp >= 2 or timestamp <= 4")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 6)
    }
  }

  test("receive a select containing a = selection") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select name from ${LongMetric.name} where timestamp = 2 ")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 1)
    }
  }

  test("receive a select containing a GTE AND a = selection") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select name from ${LongMetric.name} where timestamp >= 2 and name = John")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 1)
    }
  }

  test("receive a select containing a GTE selection and a group by") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select sum(value) from ${LongMetric.name} where timestamp >= 3 group by name order by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 4L, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 5L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 6L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 3L, Map.empty, Map("name" -> "J"))
        ))
    }
  }

  test("receive a select containing a LTE selection and a group by using first aggregation functions") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select first(value) from ${LongMetric.name} where timestamp <= 8 group by name order by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(6L, 4L, Map.empty, Map("name" -> "Bill")),
          Bit(8L, 5L, Map.empty, Map("name" -> "Frank")),
          Bit(4L, 3L, Map.empty, Map("name" -> "J")),
          Bit(1L, 1L, Map.empty, Map("name" -> "John"))
        ))
    }
  }

  test("receive a select containing a LTE selection and a group by using last aggregation functions") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select last(value) from ${LongMetric.name} where timestamp <= 8 group by name order by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(6L, 4L, Map.empty, Map("name" -> "Bill")),
          Bit(8L, 5L, Map.empty, Map("name" -> "Frank")),
          Bit(4L, 3L, Map.empty, Map("name" -> "J")),
          Bit(2L, 2L, Map.empty, Map("name" -> "John"))
        ))
    }
  }

  test("fail if receive a select containing a GTE selection and a group by without any aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select creationDate from ${LongMetric.name} where timestamp => 2 group by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  test("receive a select containing a non existing entity") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select creationDate from nonexisting where timestamp => 2 group by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  test("receive a group by statement with asc ordering over string dimension") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select count(value) from ${LongMetric.name} group by name order by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 1L, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 1L, Map.empty, Map("name" -> "J")),
          Bit(0L, 2L, Map.empty, Map("name" -> "John"))
        ))

      val distinctQuery = nsdb
        .db(db)
        .namespace(namespace).metric(DoubleMetric.name)
        .query(s"select count(distinct value) from ${DoubleMetric.name} group by name order by name")

      val distinctReadRes = Await.result(nsdb.execute(distinctQuery), 10.seconds)

      assert(
        distinctReadRes.records.map(_.asBit) == Seq(
          Bit(0L, 1L, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 1L, Map.empty, Map("name" -> "John"))
        ))
    }
  }

  test("receive a group by statement with desc ordering over string dimension") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val countQuery = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select count(value) from ${LongMetric.name} group by name order by name desc")

      var readRes = Await.result(nsdb.execute(countQuery), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 2L, Map.empty, Map("name" -> "John")),
          Bit(0L, 1L, Map.empty, Map("name" -> "J")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Bill"))
        ))

      val countDistinctQuery = nsdb
        .db(db)
        .namespace(namespace).metric(DoubleMetric.name)
        .query(s"select count(distinct value) from ${DoubleMetric.name} group by name order by name desc")

       readRes = Await.result(nsdb.execute(countDistinctQuery), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 1L, Map.empty, Map("name" -> "John")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Bill"))
        ))

      val sumQuery = nsdb
        .db(db)
        .namespace(namespace).metric(DoubleMetric.name)
        .query(s"select sum(value) from ${DoubleMetric.name} group by name order by name desc")

      readRes = Await.result(nsdb.execute(sumQuery), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 3.0, Map.empty, Map("name" -> "John")),
          Bit(0L, 1.5, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 1.5, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 1.5, Map.empty, Map("name" -> "Bill"))
        ))
    }
  }

  test("execute a group by select statement with desc ordering over numerical dimension") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(LongMetric.name)
        .query(s"select sum(value) from ${LongMetric.name} group by name order by value desc")

      var readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.map(_.asBit.value.rawValue) == Seq(6, 5, 4, 3, 3))

      val doubleQuery = nsdb
        .db(db)
        .namespace(namespace)
        .metric(DoubleMetric.name)
        .query(s"select sum(value) from ${DoubleMetric.name} group by name order by value desc")

      readRes = Await.result(nsdb.execute(doubleQuery), 10.seconds)

      assert(readRes.records.map(_.asBit.value.rawValue) == Seq(3.0, 1.5, 1.5, 1.5))
    }
  }

  test("receive a select containing a group by on long dimension with count aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(AggregationLongMetric.name)
        .query(s"select count(value) from ${AggregationLongMetric.name} group by age order by value")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) ==
          Seq(Bit(0L, 1L, Map.empty, Map("age" -> 20L)), Bit(0L, 5L, Map.empty, Map("age" -> 15L))))
    }
  }

  test("receive a select containing a group by on long dimension with count distinct aggregation on the value") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(AggregationLongMetric.name)
        .query(s"select count(distinct value) from ${AggregationLongMetric.name} group by age order by value")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) ==
          Seq(Bit(0L, 1L, Map.empty, Map("age" -> 20L)), Bit(0L, 3L, Map.empty, Map("age" -> 15L))))
    }
  }

  test("receive a select containing a group by with count distinct aggregation on a numeric tag") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(AggregationLongMetric.name)
        .query(s"select count(distinct height) from ${AggregationLongMetric.name} group by name order by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) ==
          Seq(Bit(0,1L,Map(),Map("name" -> "Bill"),Set()),
            Bit(0,1L,Map(),Map("name" -> "Frank"),Set()),
            Bit(0,1L,Map(),Map("name" -> "Frankie"),Set()),
            Bit(0,1L,Map(),Map("name" -> "John"),Set()))
      )
    }
  }

  test("receive a select containing a group by with count distinct aggregation on a string tag") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(AggregationLongMetric.name)
        .query(s"select count(distinct name) from ${AggregationLongMetric.name} group by age order by age")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0,4L,Map(),Map("age" -> 15L),Set()),
          Bit(0,1L,Map(),Map("age" -> 20L),Set())
        )
      )
    }
  }

  test("receive a select containing a group by on long dimension with sum aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .metric(AggregationLongMetric.name)
        .query(s"select sum(value) from ${AggregationLongMetric.name} group by age order by age")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) ==
          Seq(
            Bit(0L, 9L, Map.empty, Map("age" -> 15L)),
            Bit(0L, 4L, Map.empty, Map("age" -> 20L))
          ))
    }
  }

  test("receive a select containing a group by on long dimension with max and min aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val maxQuery = nsdb
        .db(db)
        .namespace(namespace)
        .metric(AggregationLongMetric.name)
        .query(s"select max(value) from ${AggregationLongMetric.name} group by age order by age")

      val maxResponse = Await.result(nsdb.execute(maxQuery), 10.seconds)

      assert(
        maxResponse.records.map(_.asBit) ==
          Seq(
            Bit(0L, 3L, Map.empty, Map("age" -> 15L)),
            Bit(0L, 4L, Map.empty, Map("age" -> 20L))
          ))

      val minQuery = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select min(value) from ${AggregationLongMetric.name} group by age order by age")

      val minResponse = Await.result(nsdb.execute(minQuery), 10.seconds)

      assert(
        minResponse.records.map(_.asBit) ==
          Seq(
            Bit(0L, 1L, Map.empty, Map("age" -> 15L)),
            Bit(0L, 4L, Map.empty, Map("age" -> 20L))
          ))
    }
  }

  test("receive a select containing a group by on long dimension with first and last aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val firstQuery = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select first(value) from ${AggregationLongMetric.name} group by age order by age")

      val firstResponse = Await.result(nsdb.execute(firstQuery), 10.seconds)

      assert(
        firstResponse.records.map(_.asBit) ==
          Seq(
            Bit(1L, 2L, Map.empty, Map("age" -> 15L)),
            Bit(4L, 4L, Map.empty, Map("age" -> 20L))
          ))

      val lastQuery = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select last(value) from ${AggregationLongMetric.name} group by age order by age")

      val lastResponse = Await.result(nsdb.execute(lastQuery), 10.seconds)

      assert(
        lastResponse.records.map(_.asBit) ==
          Seq(
            Bit(10L, 2L, Map.empty, Map("age" -> 15L)),
            Bit(4L, 4L, Map.empty, Map("age"  -> 20L))
          ))
    }
  }

  test("receive a select containing a group by on double dimension with count aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select count(value) from ${AggregationLongMetric.name} group by height order by value desc")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 3L, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 2L, Map.empty, Map("height" -> 32.0)),
          Bit(0L, 1L, Map.empty, Map("height" -> 31.0))
        ))
    }
  }

  test("receive a select containing a group by on double dimension with count distinct aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select count(distinct value) from ${AggregationLongMetric.name} group by height order by value desc")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 3L, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 2L, Map.empty, Map("height" -> 32.0)),
          Bit(0L, 1L, Map.empty, Map("height" -> 31.0))
        ))
    }
  }

  test("receive a select containing a group by on double dimension with sum aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select sum(value) from ${AggregationLongMetric.name} group by height order by height")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 9L, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 1L, Map.empty, Map("height" -> 31.0)),
          Bit(0L, 3L, Map.empty, Map("height" -> 32.0))
        ))
    }
  }

  test("receive a select containing a group by on double dimension with max and min aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val maxRequest = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select max(value) from ${AggregationLongMetric.name} group by height order by height")

      val maxResponse = Await.result(nsdb.execute(maxRequest), 10.seconds)

      assert(
        maxResponse.records.map(_.asBit) == Seq(
          Bit(0L, 4L, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 1L, Map.empty, Map("height" -> 31.0)),
          Bit(0L, 2L, Map.empty, Map("height" -> 32.0))
        ))

      val minRequest = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select min(value) from ${AggregationLongMetric.name} group by height order by height")

      val minResponse = Await.result(nsdb.execute(minRequest), 10.seconds)

      assert(
        minResponse.records.map(_.asBit) == Seq(
          Bit(0L, 2L, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 1L, Map.empty, Map("height" -> 31.0)),
          Bit(0L, 1L, Map.empty, Map("height" -> 32.0))
        ))
    }
  }

  test("receive a select containing a group by on double dimension with last and first aggregation") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val firstRequest = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select first(value) from ${AggregationLongMetric.name} group by height order by height")

      val firstResponse = Await.result(nsdb.execute(firstRequest), 10.seconds)

      assert(
        firstResponse.records.map(_.asBit) == Seq(
          Bit(1L, 2L, Map.empty, Map("height" -> 30.5)),
          Bit(6L, 1L, Map.empty, Map("height" -> 31.0)),
          Bit(8L, 1L, Map.empty, Map("height" -> 32.0))
        ))

      val lastRequest = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select last(value) from ${AggregationLongMetric.name} group by height order by height")

      val lastResponse = Await.result(nsdb.execute(lastRequest), 10.seconds)

      assert(
        lastResponse.records.map(_.asBit) == Seq(
          Bit(4L, 4L, Map.empty, Map("height"  -> 30.5)),
          Bit(6L, 1L, Map.empty, Map("height"  -> 31.0)),
          Bit(10L, 2L, Map.empty, Map("height" -> 32.0))
        ))
    }
  }

  test("receive a select containing a group by on double, long and string dimension with avg aggregation on a long value metric") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val avgDoubleRequest = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select avg(*) from ${AggregationLongMetric.name} group by height order by height")

      val firstResponse = Await.result(nsdb.execute(avgDoubleRequest), 10.seconds)

      assert(
        firstResponse.records.map(_.asBit) == Seq(
          Bit(0L, 3.0, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 1.0, Map.empty, Map("height" -> 31.0)),
          Bit(0L, 1.5, Map.empty, Map("height" -> 32.0))
        ))

      val avgLongRequest = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select avg(value) from ${AggregationLongMetric.name} group by age order by age")

      val lastResponse = Await.result(nsdb.execute(avgLongRequest), 10.seconds)

      assert(
        lastResponse.records.map(_.asBit) == Seq(
          Bit(0L, 1.8, Map.empty, Map("age" -> 15L)),
          Bit(0L, 4.0, Map.empty, Map("age" -> 20L))
        ))

      val query = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationLongMetric.name)
        .query(s"select avg(value) from ${AggregationLongMetric.name} where timestamp >= 3 group by name order by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 1.0, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 1.0, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 2.0, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 4.0, Map.empty, Map("name" -> "John"))
        ))
    }
  }

  test("receive a select containing a group by on double, long and string dimension with avg aggregation on a double value metric") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val avgDoubleRequest = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationDoubleMetric.name)
        .query(s"select avg(*) from ${AggregationDoubleMetric.name} group by height order by height")

      val firstResponse = Await.result(nsdb.execute(avgDoubleRequest), 10.seconds)

      assert(
        firstResponse.records.map(_.asBit) == Seq(
          Bit(0L, 3.0, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 1.0, Map.empty, Map("height" -> 31.0)),
          Bit(0L, 1.5, Map.empty, Map("height" -> 32.0))
        ))

      val avgLongRequest = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationDoubleMetric.name)
        .query(s"select avg(value) from ${AggregationDoubleMetric.name} group by age order by age")

      val lastResponse = Await.result(nsdb.execute(avgLongRequest), 10.seconds)

      assert(
        lastResponse.records.map(_.asBit) == Seq(
          Bit(0L, 1.8, Map.empty, Map("age" -> 15L)),
          Bit(0L, 4.0, Map.empty, Map("age" -> 20L))
        ))

      val query = nsdb
        .db(db)
        .namespace(namespace).metric(AggregationDoubleMetric.name)
        .query(s"select avg(value) from ${AggregationDoubleMetric.name} where timestamp >= 3 group by name order by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 1.0, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 1.0, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 2.0, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 4.0, Map.empty, Map("name" -> "John"))
        ))
    }
  }
}
