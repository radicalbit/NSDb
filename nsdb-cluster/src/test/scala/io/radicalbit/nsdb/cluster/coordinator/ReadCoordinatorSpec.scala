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

package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.AddRecordToLocation
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.commands.{DeleteNamespaceSchema, WarmUpSchemas}
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.events.NamespaceSchemaDeleted
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, DECIMAL, VARCHAR}
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

object LongMetric {

  val name = "longMetric"

  val recordsShard1: Seq[Bit] = Seq(
    Bit(1L, 1L, Map("surname" -> "Doe"), Map("name" -> "John")),
    Bit(2L, 1L, Map("surname" -> "Doe"), Map("name" -> "John")),
    Bit(4L, 1L, Map("surname" -> "D"), Map("name"   -> "J"))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(6L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(8L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(10L, 1L, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
  )

  val testRecords = recordsShard1 ++ recordsShard2
}

object DoubleMetric {

  val name = "doubleMetric"

  val recordsShard1: Seq[Bit] = Seq(
    Bit(2L, 1.5, Map("surname" -> "Doe"), Map("name" -> "John")),
    Bit(4L, 1.5, Map("surname" -> "Doe"), Map("name" -> "John"))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(6L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(8L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(10L, 1.5, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
  )

  val testRecords = recordsShard1 ++ recordsShard2
}

object AggregationMetric {

  val name = "aggregationMetric"

  val recordsShard1: Seq[Bit] = Seq(
    Bit(2L, 2L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 15L, "height" -> 30.5)),
    Bit(4L, 2L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 20L, "height" -> 30.5))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(2L, 1L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
    Bit(6L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
    Bit(8L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
    Bit(10L, 1L, Map("surname" -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
  )

  val testRecords = recordsShard1 ++ recordsShard2
}

class FakeSchemaCache extends Actor {

  val schemas: mutable.Map[(String, String, String), Schema] = mutable.Map.empty

  def receive: Receive = {
    case PutSchemaInCache(db, namespace, metric, value) =>
      schemas.put((db, namespace, metric), value)
      sender ! SchemaCached(db, namespace, metric, Some(value))
    case GetSchemaFromCache(db, namespace, metric) =>
      sender ! SchemaCached(db, namespace, metric, schemas.get((db, namespace, metric)))
    case EvictSchema(db, namespace, metric) =>
      schemas -= ((db, namespace, metric))
      sender ! SchemaCached(db, namespace, metric, None)
    case DeleteNamespaceSchema(db, namespace) =>
//      sender ! NamespaceSchemaDeleted(db, namespace)
      schemas.clear()
      sender ! NamespaceSchemaDeleted(db, namespace)

  }
}

class ReadCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "ReadCoordinatorSpec",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
      ))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with WriteInterval {

  val probe     = TestProbe()
  val basePath  = "target/test_index/ReadCoordinatorShardSpec"
  val db        = "db"
  val namespace = "registry"
  val schemaCoordinator = system.actorOf(
    SchemaCoordinator.props(basePath, system.actorOf(Props[FakeSchemaCache]), system.actorOf(Props.empty)))
  val metricsDataActor     = system.actorOf(MetricsDataActor.props(basePath))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(null, schemaCoordinator)

  override def beforeAll = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(5 second)

    readCoordinatorActor ! WarmUpCompleted
    schemaCoordinator ! WarmUpSchemas(List.empty)

    Await.result(readCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, "node1"), 10 seconds)

    val location1 = Location(_: String, "node1", 0, 5)
    val location2 = Location(_: String, "node1", 6, 10)

    //long metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, LongMetric.name), 10 seconds)

    Await.result(
      schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, LongMetric.name, LongMetric.testRecords.head),
      10 seconds)

    LongMetric.recordsShard1.foreach(r =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(LongMetric.name)), 10 seconds))
    LongMetric.recordsShard2.foreach(r =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(LongMetric.name)), 10 seconds))

    //double metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, DoubleMetric.name), 10 seconds)

    Await.result(
      schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, DoubleMetric.name, DoubleMetric.testRecords.head),
      10 seconds)

    DoubleMetric.recordsShard1.foreach(r =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(DoubleMetric.name)), 10 seconds))
    DoubleMetric.recordsShard2.foreach(r =>
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(DoubleMetric.name)), 10 seconds))

    //aggregation metric
    Await.result(metricsDataActor ? DropMetric(db, namespace, AggregationMetric.name), 10 seconds)

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
                                                            namespace,
                                                            AggregationMetric.name,
                                                            AggregationMetric.testRecords.head),
                 10 seconds)

    AggregationMetric.recordsShard1.foreach(
      r =>
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(AggregationMetric.name)),
                     10 seconds))
    AggregationMetric.recordsShard2.foreach(
      r =>
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(AggregationMetric.name)),
                     10 seconds))

    expectNoMessage(interval)
    expectNoMessage(interval)
  }

  "ReadCoordinator" when {

    "receive a select projecting a wildcard with a limit" should {
      "execute it successfully" in within(5.seconds) {

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = AllFields,
                               limit = Some(LimitOperator(2)))
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.size shouldBe 2

      }
    }

    "receive a select projecting a wildcard with a limit and a ordering" should {
      "execute it successfully when ordered by timestamp" in within(5.seconds) {

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = AllFields,
              limit = Some(LimitOperator(2)),
              order = Some(DescOrderOperator("timestamp"))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size shouldBe 2
        expected.values shouldBe LongMetric.recordsShard2.tail.reverse
      }

      "execute it successfully when ordered by another dimension" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = AllFields,
                               limit = Some(LimitOperator(3)),
                               order = Some(DescOrderOperator("name")))
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size shouldBe 3
        LongMetric.recordsShard1 foreach { r =>
          expected.values.contains(r) shouldBe true
        }
      }
    }

    "receive a GetDbs" should {
      "return it properly" in within(5.seconds) {
        probe.send(readCoordinatorActor, GetDbs)

        val expected = probe.expectMsgType[DbsGot]
        expected.dbs shouldBe Set(db)

      }
    }

    "receive a GetNamespace" should {
      "return it properly" in within(5.seconds) {
        probe.send(readCoordinatorActor, GetNamespaces(db))

        val expected = probe.expectMsgType[NamespacesGot]
        expected.namespaces shouldBe Set(namespace)

      }
    }

    "receive a GetMetrics given a namespace" should {
      "return it properly" in within(5.seconds) {
        probe.send(readCoordinatorActor, GetMetrics(db, namespace))

        val expected = probe.expectMsgType[MetricsGot]

        expected.namespace shouldBe namespace
        expected.metrics shouldBe Set(LongMetric.name, DoubleMetric.name, AggregationMetric.name)

      }
    }

    "receive a GetSchema given a namespace and a metric" should {
      "return it properly" in within(5.seconds) {
        probe.send(readCoordinatorActor, GetSchema(db, namespace, LongMetric.name))

        awaitAssert {
          val expected = probe.expectMsgType[SchemaGot]
          expected.namespace shouldBe namespace
          expected.metric shouldBe LongMetric.name
          expected.schema shouldBe defined

          expected.schema.get.fields.toSeq.sortBy(_.name) shouldBe
            Seq(
              SchemaField("name", TagFieldType, VARCHAR()),
              SchemaField("surname", DimensionFieldType, VARCHAR()),
              SchemaField("timestamp", TimestampFieldType, BIGINT()),
              SchemaField("value", ValueFieldType, BIGINT())
            )
        }

        probe.send(readCoordinatorActor, GetSchema(db, namespace, DoubleMetric.name))

        awaitAssert {
          val expected = probe.expectMsgType[SchemaGot]
          expected.namespace shouldBe namespace
          expected.metric shouldBe DoubleMetric.name
          expected.schema shouldBe defined

          expected.schema.get.fields.toSeq.sortBy(_.name) shouldBe
            Seq(
              SchemaField("name", TagFieldType, VARCHAR()),
              SchemaField("surname", DimensionFieldType, VARCHAR()),
              SchemaField("timestamp", TimestampFieldType, BIGINT()),
              SchemaField("value", ValueFieldType, DECIMAL())
            )
        }
      }
    }

    "receive a select over all fields" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = ListFields(List(Field("*", None))),
                               limit = None)
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 6
        val result = expected.values.sortBy(_.timestamp)

        for {
          i <- 0 to 5
        } {
          result(i) shouldBe (if (i < 3) LongMetric.recordsShard1(i) else LongMetric.recordsShard2(i - 3))
        }
      }
    }

    "receive a select over tags fields" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = ListFields(List(Field("name", None))),
                               limit = None)
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 6
        val names = expected.values.flatMap(_.tags.values.map(_.asInstanceOf[String]))

        names.count(_ == "Bill") shouldBe 1
        names.count(_ == "Frank") shouldBe 1
        names.count(_ == "Frankie") shouldBe 1
        names.count(_ == "J") shouldBe 1
        names.count(_ == "John") shouldBe 2
      }
    }

    "receive a select over dimensions fields" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = ListFields(List(Field("surname", None))),
                               limit = None)
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 6

        val names = expected.values.flatMap(_.dimensions.values.map(_.asInstanceOf[String]))
        names.count(_ == "Doe") shouldBe 5
        names.count(_ == "D") shouldBe 1
      }
    }

    "receive a select over dimensions and tags fields" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = ListFields(List(Field("name", None), Field("surname", None))),
                               limit = None)
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 6

        val dimensions = expected.values.flatMap(_.dimensions.values.map(_.asInstanceOf[String]))
        dimensions.count(_ == "Doe") shouldBe 5

        val tags = expected.values.flatMap(_.tags.values.map(_.asInstanceOf[String]))

        tags.count(_ == "Bill") shouldBe 1
        tags.count(_ == "Frank") shouldBe 1
        tags.count(_ == "Frankie") shouldBe 1
        tags.count(_ == "J") shouldBe 1
        tags.count(_ == "John") shouldBe 2
      }
    }

    "receive a select distinct over a single field" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = true,
              fields = ListFields(List(Field("name", None))),
              limit = Some(LimitOperator(6))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        val names = expected.values.flatMap(_.tags.values.map(_.asInstanceOf[String]))

        names.contains("Bill") shouldBe true
        names.contains("Frank") shouldBe true
        names.contains("Frankie") shouldBe true
        names.contains("John") shouldBe true
        names.contains("J") shouldBe true
        names.size shouldBe 5
      }

      "execute successfully with limit over distinct values" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = true,
              fields = ListFields(List(Field("name", None))),
              limit = Some(LimitOperator(2))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        val names = expected.values.flatMap(_.tags.values.map(_.asInstanceOf[String]))
        names.size shouldBe 2
      }

      "execute successfully with ascending order" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = true,
              fields = ListFields(List(Field("name", None))),
              order = Some(AscOrderOperator("name")),
              limit = Some(LimitOperator(6))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(0L, 0L, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 0L, Map.empty, Map("name" -> "J")),
          Bit(0L, 0L, Map.empty, Map("name" -> "John"))
        )
      }

      "execute successfully with descending order" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = true,
              fields = ListFields(List(Field("name", None))),
              order = Some(DescOrderOperator("name")),
              limit = Some(LimitOperator(6))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(0L, 0L, Map.empty, Map("name" -> "John")),
          Bit(0L, 0L, Map.empty, Map("name" -> "J")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 0L, Map.empty, Map("name" -> "Bill"))
        )
      }
    }

    "receive a select projecting a wildcard" should {
      "execute it successfully" in within(5.seconds) {

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = AllFields,
                               limit = Some(LimitOperator(6)))
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.sortBy(_.timestamp) shouldBe LongMetric.testRecords
      }
      "fail if distinct" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = true,
                               fields = AllFields,
                               limit = Some(LimitOperator(6)))
          )
        )
        awaitAssert {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }

    "receive a select projecting a list of fields" should {
      "execute it successfully with only simple fields" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None), Field("surname", None))),
              limit = Some(LimitOperator(6))
            )
          )
        )

        awaitAssert {
          val expected = probe.expectMsgType[SelectStatementExecuted]

          expected.values.sortBy(_.timestamp) shouldBe LongMetric.testRecords
        }
      }

      "execute it successfully with mixed aggregated and simple" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("*", Some(CountAggregation)), Field("name", None))),
              limit = Some(LimitOperator(6))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.sortBy(_.timestamp) shouldBe Seq(
          Bit(1L, 1, Map.empty, Map("name"  -> "John", "count(*)"    -> 6)),
          Bit(2L, 1, Map.empty, Map("name"  -> "John", "count(*)"    -> 6)),
          Bit(4L, 1, Map.empty, Map("name"  -> "J", "count(*)"       -> 6)),
          Bit(6L, 1, Map.empty, Map("name"  -> "Bill", "count(*)"    -> 6)),
          Bit(8L, 1, Map.empty, Map("name"  -> "Frank", "count(*)"   -> 6)),
          Bit(10L, 1, Map.empty, Map("name" -> "Frankie", "count(*)" -> 6))
        )
      }

      "execute it successfully with only a count" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(CountAggregation)))
              ),
              limit = Some(LimitOperator(4))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(0, 4L, Map.empty, Map("count(*)" -> 4))
        )
      }

      "fail when other aggregation than count is provided" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(CountAggregation)),
                     Field("surname", None),
                     Field("creationDate", Some(SumAggregation)))),
              limit = Some(LimitOperator(4))
            )
          )
        )
        awaitAssert {
          probe.expectMsgType[SelectStatementFailed]
        }
      }

      "fail when is select distinct" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = true,
              fields = ListFields(List(Field("name", None), Field("surname", None))),
              limit = Some(LimitOperator(5))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }

    "receive a select containing a range selection" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
              limit = Some(LimitOperator(4))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size should be(2)
      }
    }

    "receive a select containing a GTE selection" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 1
        expected.values.head shouldBe Bit(10, 1, Map.empty, Map("name" -> "Frankie"))
      }
    }

    "receive a select containing a GTE and a NOT selection" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(
                Condition(
                  UnaryLogicalExpression(
                    ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L),
                    NotOperator
                  ))),
              limit = Some(LimitOperator(6))
            )
          )
        )

        awaitAssert {
          val expected = probe.expectMsgType[SelectStatementExecuted]

          expected.values.size should be(5)
        }
      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
                operator = AndOperator,
                expression2 =
                  ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
              ))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size should be(1)
      }
    }

    "receive a select containing a GTE OR a LT selection" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
              ))),
              limit = Some(LimitOperator(6))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size should be(6)
      }
    }

    "receive a select containing a = selection" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(EqualityExpression(dimension = "timestamp", value = 2L))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size should be(1)
      }
    }

    "receive a select containing a GTE AND a = selection" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = AndOperator,
                expression2 = EqualityExpression(dimension = "name", value = "John")
              ))),
              limit = Some(LimitOperator(6))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size should be(1)
      }
    }

    "receive a select containing a GTE selection and a group by" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              condition = Some(Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
              groupBy = Some("name")
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size should be(5)
      }
    }

    "receive a select containing a GTE selection and a group by without any aggregation" should {
      "fail" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("creationDate", None))),
              condition = Some(Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
              groupBy = Some("name")
            )
          )
        )
        awaitAssert {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }

    "receive a select containing a non existing entity" should {
      "return an error message properly" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = "nonexisting",
                               distinct = false,
                               fields = AllFields,
                               limit = Some(LimitOperator(5)))
          )
        )
        awaitAssert {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }

    "receive a select containing a group by on string dimension " should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("*", Some(CountAggregation)))),
              groupBy = Some("name"),
              order = Some(AscOrderOperator("name"))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(0L, 1L, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 1L, Map.empty, Map("name" -> "J")),
          Bit(0L, 2L, Map.empty, Map("name" -> "John"))
        )
      }

      "execute it successfully with asc ordering over string dimension" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation)))),
              groupBy = Some("name"),
              order = Some(AscOrderOperator("name"))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(0L, 1L, Map.empty, Map("name" -> "Bill")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 1L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 1L, Map.empty, Map("name" -> "J")),
          Bit(0L, 2L, Map.empty, Map("name" -> "John"))
        )
      }

      "execute it successfully with desc ordering over string dimension" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some("name"),
              order = Some(DescOrderOperator("name"))
            )
          )
        )

        awaitAssert {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values shouldBe Seq(
            Bit(0L, 2, Map.empty, Map("name" -> "John")),
            Bit(0L, 1, Map.empty, Map("name" -> "J")),
            Bit(0L, 1, Map.empty, Map("name" -> "Frankie")),
            Bit(0L, 1, Map.empty, Map("name" -> "Frank")),
            Bit(0L, 1, Map.empty, Map("name" -> "Bill"))
          )
        }

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some("name"),
              order = Some(DescOrderOperator("name"))
            )
          )
        )

        awaitAssert {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values shouldBe Seq(
            Bit(0L, 3.0, Map.empty, Map("name" -> "John")),
            Bit(0L, 1.5, Map.empty, Map("name" -> "Frankie")),
            Bit(0L, 1.5, Map.empty, Map("name" -> "Frank")),
            Bit(0L, 1.5, Map.empty, Map("name" -> "Bill"))
          )
        }
      }

      "execute it successfully with desc ordering over numerical dimension" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some("name"),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value) shouldBe Seq(2, 1, 1, 1, 1)

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some("name"),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value) shouldBe Seq(3.0, 1.5, 1.5, 1.5)

      }

      "execute it successfully with asc ordering over numerical dimension" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some("name"),
              order = Some(AscOrderOperator("value")),
              limit = Some(LimitOperator(2))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value) shouldBe Seq(1, 1)

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some("name"),
              order = Some(AscOrderOperator("value")),
              limit = Some(LimitOperator(2))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value) shouldBe Seq(1.5, 1.5)
      }
    }

    "receive a select containing a group by on long dimension" should {
      "execute it successfully with count aggregation" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation)))),
              groupBy = Some("age"),
              order = Some(AscOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(Bit(0L, 1, Map.empty, Map("age" -> 20)), Bit(0L, 5, Map.empty, Map("age" -> 15)))
      }

      "execute it successfully with sum aggregation" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some("age"),
              order = Some(AscOrderOperator("age"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0L, 6L, Map.empty, Map("age" -> 15L)),
          Bit(0L, 2L, Map.empty, Map("age" -> 20L))
        )
      }
    }

    "receive a select containing a group by on double dimension" should {
      "execute it successfully with count aggregation" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation)))),
              groupBy = Some("height"),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0L, 3, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 2, Map.empty, Map("height" -> 32.0)),
          Bit(0L, 1, Map.empty, Map("height" -> 31.0))
        )
      }
    }

    "execute it successfully with sum aggregation" in within(5.seconds) {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(SumAggregation)))),
            groupBy = Some("height"),
            order = Some(AscOrderOperator("height"))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0L, 5, Map.empty, Map("height" -> 30.5)),
        Bit(0L, 1, Map.empty, Map("height" -> 31.0)),
        Bit(0L, 2, Map.empty, Map("height" -> 32.0))
      )
    }
  }
}
