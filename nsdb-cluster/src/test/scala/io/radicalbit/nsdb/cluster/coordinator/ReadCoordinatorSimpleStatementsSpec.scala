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

package io.radicalbit.nsdb.cluster.coordinator

import io.radicalbit.nsdb.cluster.coordinator.mockedData.MockedData._
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, DECIMAL, VARCHAR}
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.concurrent.duration._

class ReadCoordinatorSimpleStatementsSpec extends AbstractReadCoordinatorSpec {

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
                               fields = AllFields(),
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
              fields = AllFields(),
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

      "execute it successfully when ordered by value" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = AllFields(),
              limit = Some(LimitOperator(2)),
              order = Some(DescOrderOperator("value"))
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
                               fields = AllFields(),
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
        expected.metrics shouldBe Set(LongMetric.name,
                                      DoubleMetric.name,
                                      AggregationLongMetric.name,
                                      AggregationDoubleMetric.name)

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

          expected.schema.get.fieldsMap.values.toSeq.sortBy(_.name) shouldBe
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

          expected.schema.get.fieldsMap.values.toSeq.sortBy(_.name) shouldBe
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
        val names = expected.values.flatMap(_.tags.values.map(_.rawValue.asInstanceOf[String]))

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

        val names = expected.values.flatMap(_.dimensions.values.map(_.rawValue.asInstanceOf[String]))
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

        val dimensions = expected.values.flatMap(_.dimensions.values.map(_.rawValue.asInstanceOf[String]))
        dimensions.count(_ == "Doe") shouldBe 5

        val tags = expected.values.flatMap(_.tags.values.map(_.rawValue.asInstanceOf[String]))

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
        val names = expected.values.flatMap(_.tags.values.map(_.rawValue.asInstanceOf[String]))

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
        val names = expected.values.flatMap(_.tags.values.map(_.rawValue.asInstanceOf[String]))
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
                               fields = AllFields(),
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
                               fields = AllFields(),
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
              condition = Some(
                Condition(RangeExpression(dimension = "timestamp",
                                          value1 = AbsoluteComparisonValue(2L),
                                          value2 = AbsoluteComparisonValue(4L)))),
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
              condition = Some(
                Condition(ComparisonExpression(dimension = "timestamp",
                                               comparison = GreaterOrEqualToOperator,
                                               value = AbsoluteComparisonValue(10L)))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 1
        expected.values.head shouldBe Bit(10, 6L, Map.empty, Map("name" -> "Frankie"))
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
                  NotExpression(
                    ComparisonExpression(dimension = "timestamp",
                                         comparison = GreaterOrEqualToOperator,
                                         value = AbsoluteComparisonValue(10L))
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
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(2L)),
                operator = AndOperator,
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessOrEqualToOperator,
                                                   value = AbsoluteComparisonValue(4L))
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
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterOrEqualToOperator,
                                                   value = AbsoluteComparisonValue(2L)),
                operator = OrOperator,
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessThanOperator,
                                                   value = AbsoluteComparisonValue(4L))
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
              condition =
                Some(Condition(EqualityExpression(dimension = "timestamp", value = AbsoluteComparisonValue(2L)))),
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
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterOrEqualToOperator,
                                                   value = AbsoluteComparisonValue(2L)),
                operator = AndOperator,
                expression2 = EqualityExpression(dimension = "name", value = AbsoluteComparisonValue("John"))
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
  }
}
