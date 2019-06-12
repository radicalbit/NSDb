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

package io.radicalbit.nsdb.cluster

import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.client.rpc.converter.GrpcBitConverters._
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.minicluster.MiniClusterSpec
import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object LongMetric {

  val name = "longMetric"

  val testRecords: List[Bit] = List(
    Bit(1L, 1L, Map("surname"  -> "Doe"), Map("name" -> "John")),
    Bit(2L, 2L, Map("surname"  -> "Doe"), Map("name" -> "John")),
    Bit(4L, 3L, Map("surname"  -> "D"), Map("name"   -> "J")),
    Bit(6L, 4L, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(8L, 5L, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(10L, 6L, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
  )

}

object DoubleMetric {

  val name = "doubleMetric"

  val testRecords: List[Bit] = List(
    Bit(2L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "John")),
    Bit(4L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "John")),
    Bit(6L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(8L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(10L, 1.5, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
  )

}

object AggregationMetric {

  val name = "aggregationMetric"

  val testRecords: List[Bit] = List(
    Bit(2L, 2L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
    Bit(4L, 2L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 20L, "height" -> 30.5)),
    Bit(2L, 1L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
    Bit(6L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
    Bit(8L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
    Bit(10L, 1L, Map("surname" -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
  )

}

class ReadCoordinatorClusterSpec extends MiniClusterSpec {

  override val nodesNumber: Int = 3

  val db        = "db"
  val namespace = "registry"

  override def beforeAll(): Unit = {

    val firstNode = minicluster.nodes.head

    val nsdb =
      Await.result(NSDB.connect(host = "127.0.0.1", port = firstNode.grpcPort)(ExecutionContext.global), 10.seconds)

    super.beforeAll()

    Await.result(nsdb.write(LongMetric.testRecords.map(_.asApiBit(db, namespace, LongMetric.name))), 10.seconds)
    Await.result(nsdb.write(DoubleMetric.testRecords.map(_.asApiBit(db, namespace, DoubleMetric.name))), 10.seconds)
    Await.result(nsdb.write(AggregationMetric.testRecords.map(_.asApiBit(db, namespace, AggregationMetric.name))),
                 10.seconds)

    waitIndexing()
    waitIndexing()
  }

  override def afterAll(): Unit = {
    minicluster.stop()
  }

  test("receive a select projecting a wildcard with a limit") {

    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from ${LongMetric.name} limit 2")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.completedSuccessfully)
      assert(readRes.records.size == 2)
    }
  }

  test("receive a select projecting a wildcard with a limit and a ordering when ordered by timestamp") {

    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from ${LongMetric.name} order by timestamp desc limit 2")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.completedSuccessfully)
      assert(readRes.records.size == 2)
      assert(readRes.records.map(_.asBit) == LongMetric.testRecords.reverse.take(2))
    }
  }

  test("execute it successfully when ordered by value") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from ${LongMetric.name} order by value desc limit 2")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.completedSuccessfully)
      assert(readRes.records.size == 2)

      val results = readRes.records.map(_.asBit)
      results.foreach(r => assert(LongMetric.testRecords.takeRight(2).contains(r)))

    }
  }

  test("receive a select projecting a wildcard with a limit and a ordering when ordered by another dimension") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from ${LongMetric.name} order by name desc limit 3")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.completedSuccessfully)
      assert(readRes.records.size == 3)

      val results = readRes.records.map(_.asBit)
      results.foreach(r => assert(LongMetric.testRecords.take(3).contains(r)))
    }
  }

  test("receive a select over tags fields") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
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
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select surname from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 6)

      val names = readRes.records.flatMap(_.dimensions.values.map(_.getStringValue))
      assert(names.count(_ == "Doe") == 5)
      assert(names.count(_ == "D") == 1)
    }
  }

  test("receive a select over dimensions and tags fields") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
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
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
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
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select distinct name from ${LongMetric.name} limit 2")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 2)
    }
  }

  test("execute successfully with ascending order") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
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
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
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
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select distinct * from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  //the more i think about this feature, the more it seems unnecessary and misleading
  ignore("receive a select projecting a list of fields with mixed aggregated and simple") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
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
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.size == 1)

      assert(readRes.records.head.asBit == Bit(0, 6L, Map.empty, Map("count(*)" -> 6)))
    }
  }

  test("fail if receive a select projecting a list of fields when other aggregation than count is provided") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*), surname, sum(creationDate) from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  test("fail if receive a select distinct projecting a list of fields ") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select distinct name, surname from ${LongMetric.name}")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  test("receive a select containing a range selection") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select name from ${LongMetric.name} where timestamp in (2,4) limit 4")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.size == 2)
    }
  }

  test("receive a select containing a GTE selection") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select name from ${LongMetric.name} where timestamp >= 10")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 1)
      assert(readRes.records.head.asBit == Bit(10, 6, Map.empty, Map("name" -> "Frankie")))
    }
  }

  test("receive a select containing a GTE and a NOT selection") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select name from ${LongMetric.name} where not timestamp >= 10")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 5)
    }
  }

  test("receive a select containing a GT AND a LTE selection") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select name from ${LongMetric.name} where timestamp > 2 and timestamp <= 4 limit 4")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 1)
    }
  }

  test("receive a select containing a GTE OR a LT selection") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select name from ${LongMetric.name} where timestamp >= 2 or timestamp <= 4")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 6)
    }
  }

  test("receive a select containing a = selection") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select name from ${LongMetric.name} where timestamp = 2 ")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 1)
    }
  }

  test("receive a select containing a GTE AND a = selection") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select name from ${LongMetric.name} where timestamp >= 2 and name = John")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)
      assert(readRes.records.size == 1)
    }
  }

  test("receive a select containing a GTE selection and a group by") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select sum(value) from ${LongMetric.name} where timestamp >= 2 group by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.size == 5)
    }
  }

  test("fail if receive a select containing a GTE selection and a group by without any aggregation") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select creationDate from ${LongMetric.name} where timestamp => 2 group by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  test("receive a select containing a non existing entity") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select creationDate from nonexisting where timestamp => 2 group by name")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(!readRes.completedSuccessfully)
    }
  }

  test("receive a group by statement with asc ordering over string dimension") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
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
    }
  }

  test("receive a group by statement with desc ordering over string dimension") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(value) from ${LongMetric.name} group by name order by name desc")

      var readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 2, Map.empty, Map("name" -> "John")),
          Bit(0L, 1, Map.empty, Map("name" -> "J")),
          Bit(0L, 1, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 1, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 1, Map.empty, Map("name" -> "Bill"))
        ))

      val sumQuery = nsdb
        .db(db)
        .namespace(namespace)
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
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select sum(value) from ${LongMetric.name} group by name order by value desc")

      var readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(readRes.records.map(_.asBit.value.asInstanceOf[Long]) == Seq(6, 5, 4, 3, 3))

      val doubleQuery = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select sum(value) from ${DoubleMetric.name} group by name order by value desc")

      readRes = Await.result(nsdb.execute(doubleQuery), 10.seconds)

      assert(readRes.records.map(_.asBit.value.asInstanceOf[Double]) == Seq(3.0, 1.5, 1.5, 1.5))
    }
  }

  test("receive a select containing a group by on long dimension with count aggregation") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(value) from ${AggregationMetric.name} group by age order by value")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) ==
          Seq(Bit(0L, 1, Map.empty, Map("age" -> 20)), Bit(0L, 5, Map.empty, Map("age" -> 15))))
    }
  }

  test("receive a select containing a group by on long dimension with sum aggregation") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select sum(value) from ${AggregationMetric.name} group by age order by age")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) ==
          Seq(
            Bit(0L, 6L, Map.empty, Map("age" -> 15L)),
            Bit(0L, 2L, Map.empty, Map("age" -> 20L))
          ))
    }
  }

  test("receive a select containing a group by on double dimension with count aggregation") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(value) from ${AggregationMetric.name} group by height order by value desc")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 3, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 2, Map.empty, Map("height" -> 32.0)),
          Bit(0L, 1, Map.empty, Map("height" -> 31.0))
        ))
    }
  }

  test("receive a select containing a group by on double dimension with sum aggregation") {
    minicluster.nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = "127.0.0.1", port = n.grpcPort)(ExecutionContext.global), 10.seconds)
      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select sum(value) from ${AggregationMetric.name} group by height order by height")

      val readRes = Await.result(nsdb.execute(query), 10.seconds)

      assert(
        readRes.records.map(_.asBit) == Seq(
          Bit(0L, 5, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 1, Map.empty, Map("height" -> 31.0)),
          Bit(0L, 2, Map.empty, Map("height" -> 32.0))
        ))
    }
  }
}
