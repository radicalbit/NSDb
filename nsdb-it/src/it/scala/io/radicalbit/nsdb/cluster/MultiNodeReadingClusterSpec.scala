package io.radicalbit.nsdb.cluster

import io.radicalbit.nsdb.client.rpc.converter.GrpcBitConverters._
import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter
import akka.cluster.{Cluster, MemberStatus}
import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.common.NSDbType
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.test.MiniClusterSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object CustomMetric {
  val name = "custom"
  val testRecords: List[Bit] = List(
    Bit(100L, 2L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
    Bit(400L, 4L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 20L, "height" -> 30.5)),
    Bit(200L, 3L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
    Bit(600L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
    Bit(800L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
    Bit(1000L, 2L, Map("surname" -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
  )
}

/**
 * TODO: we need to have a better test coverage here
 */
class MultiNodeReadingClusterSpec extends MiniClusterSpec {

  import CustomMetric._

  val db        = "db"
  val namespace = "registry"

  override def beforeAll(): Unit = {

    super.beforeAll()

    val firstNode = nodes.head

    val nsdbConnection =
      eventually {
        Await.result(NSDB.connect(host = firstNode.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      }

    CustomMetric.testRecords.map(_.asApiBit(db, namespace, name)).foreach { bit =>
      eventually {
        assert(Await.result(nsdbConnection.write(bit), 10.seconds).completedSuccessfully)
      }
    }

    waitIndexing()
  }

  test("join cluster") {
    eventually {
      assert(
        Cluster(nodes.head.system).state.members
          .count(_.status == MemberStatus.Up) == nodes.size)
    }
  }

  test("select all order by timestamp") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from $name order by timestamp")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(
          readRes.records.map(_.asBit) == List(
            Bit(100L, 2L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
            Bit(200L, 3L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
            Bit(400L, 4L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 20L, "height" -> 30.5)),
            Bit(600L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
            Bit(800L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
            Bit(1000L, 2L, Map("surname" -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
          ))
      }
    }
  }

  test("select all order by value") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from $name order by value")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(readRes.records.map(_.asBit).map(_.value.rawValue) == List(1L, 1L, 2L, 2L, 3L, 4L))
      }
    }
  }

  test("select all order by tag") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from $name order by name")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 6)
        assert(
          readRes.records.map(_.asBit).map(_.tags.getOrElse("name", NSDbType("")).rawValue) == List("Bill",
                                                                                                    "Frank",
                                                                                                    "Frankie",
                                                                                                    "John",
                                                                                                    "John",
                                                                                                    "John"))
      }
    }
  }

  test("select all order by timestamp with limit") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from $name order by timestamp limit 2")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(
          readRes.records.map(_.asBit) == List(
            Bit(100L, 2L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 15L, "height" -> 30.5)),
            Bit(200L, 3L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 15L, "height" -> 30.5))
          ))
      }
    }
  }

  test("select all order by value with limit") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from $name order by value limit 2")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(
          readRes.records.map(_.asBit) == List(
            Bit(800L, 1L, Map("surname" -> "Doe"), Map("name" -> "Frank", "age" -> 15L, "height" -> 32.0)),
            Bit(600L, 1L, Map("surname" -> "Doe"), Map("name" -> "Bill", "age"  -> 15L, "height" -> 31.0))
          ))
      }
    }
  }

  test("select all order by tag with limit") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select * from $name order by name limit 2")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 2)
        assert(
          readRes.records.map(_.asBit) == List(
            Bit(600L, 1L, Map("surname" -> "Doe"), Map("name" -> "Bill", "age"  -> 15L, "height" -> 31.0)),
            Bit(800L, 1L, Map("surname" -> "Doe"), Map("name" -> "Frank", "age" -> 15L, "height" -> 32.0))
          ))
      }
    }
  }

  test("count all records") {
    nodes.foreach { n =>
      val nsdb =
        Await.result(NSDB.connect(host = n.hostname, port = 7817)(ExecutionContext.global), 10.seconds)

      val query = nsdb
        .db(db)
        .namespace(namespace)
        .query(s"select count(*) from $name")

      eventually {
        val readRes = Await.result(nsdb.execute(query), 10.seconds)
        assert(readRes.completedSuccessfully)
        assert(readRes.records.size == 1)
        assert(readRes.records.head.asBit.value.rawValue == 6L)
      }
    }
  }
}
