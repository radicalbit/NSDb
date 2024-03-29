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
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

class ReadCoordinatorAggregatedStatementsSpec extends AbstractReadCoordinatorSpec {

  "ReadCoordinator" when {

    "receive a select containing a GTE selection and a group by" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              condition = Some(
                Condition(
                  ComparisonExpression(dimension = "timestamp",
                                       comparison = GreaterOrEqualToOperator,
                                       value = AbsoluteComparisonValue(2L)))),
              groupBy = Some(SimpleGroupByAggregation("name"))
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
      "fail" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("creationDate", None))),
              condition = Some(
                Condition(
                  ComparisonExpression(dimension = "timestamp",
                                       comparison = GreaterOrEqualToOperator,
                                       value = AbsoluteComparisonValue(2L)))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          )
        )
        awaitAssert {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }

    "receive a select containing a non existing entity" should {
      "return an error message properly" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = "nonexisting",
                               distinct = false,
                               fields = AllFields(),
                               limit = Some(LimitOperator(5)))
          )
        )
        awaitAssert {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }

    "receive a select containing a group by on string dimension " should {
      "execute it successfully when count(*) is used instead of value" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("*", Some(CountAggregation("*"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
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

      "execute it successfully with asc ordering over string dimension" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
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

      "execute it successfully with desc ordering over string dimension" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              order = Some(DescOrderOperator("name"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0L, 3L, Map.empty, Map("name" -> "John")),
          Bit(0L, 3L, Map.empty, Map("name" -> "J")),
          Bit(0L, 6L, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 5L, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 4L, Map.empty, Map("name" -> "Bill"))
        )

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
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

      "execute it successfully with desc ordering over numerical dimension" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value.rawValue) shouldBe Seq(6, 5, 4, 3, 3)

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value.rawValue) shouldBe Seq(3.0, 1.5, 1.5, 1.5)

      }

      "execute a count successfully with asc ordering over numerical dimension" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              order = Some(AscOrderOperator("value")),
              limit = Some(LimitOperator(2))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value.rawValue) shouldBe Seq(1, 1)
      }

      "execute it successfully with asc ordering over numerical dimension" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              order = Some(AscOrderOperator("value")),
              limit = Some(LimitOperator(2))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value.rawValue) shouldBe Seq(3, 3)

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              order = Some(AscOrderOperator("value")),
              limit = Some(LimitOperator(2))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value.rawValue) shouldBe Seq(1.5, 1.5)
      }
    }

    "receive a select containing a group by on long dimension" should {
      "execute it successfully with count aggregation" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("age")),
              order = Some(AscOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0, 1L, Map(), Map("age" -> 16L), Set()),
          Bit(0, 2L, Map(), Map("age" -> 20L), Set()),
          Bit(0, 5L, Map(), Map("age" -> 15L), Set())
        )
      }

      "execute it successfully with sum aggregation" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("age")),
              order = Some(AscOrderOperator("age"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0, 14L, Map(), Map("age" -> 15L), Set()),
          Bit(0, 5L, Map(), Map("age"  -> 16L), Set()),
          Bit(0, 6L, Map(), Map("age"  -> 20L), Set())
        )
      }
    }

    "receive a select containing a group by on double dimension" should {
      "execute it successfully with count aggregation" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("height")),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0L, 4L, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 3L, Map.empty, Map("height" -> 32.0)),
          Bit(0L, 2L, Map.empty, Map("height" -> 31.0))
        )
      }
    }

    "execute it successfully with sum aggregation" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationLongMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height"))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0L, 10L, Map.empty, Map("height" -> 30.5)),
        Bit(0L, 10L, Map.empty, Map("height" -> 31.0)),
        Bit(0L, 6L, Map.empty, Map("height"  -> 32.0))
      )
    }

    "execute it successfully with first aggregation" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationLongMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(FirstAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height"))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(2L, 2L, Map.empty, Map("height" -> 30.5)),
        Bit(6L, 5L, Map.empty, Map("height" -> 31.0)),
        Bit(8L, 1L, Map.empty, Map("height" -> 32.0))
      )
    }

    "execute it successfully with last aggregation" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationLongMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(LastAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height"))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(5L, 3L, Map.empty, Map("height"  -> 30.5)),
        Bit(7L, 5L, Map.empty, Map("height"  -> 31.0)),
        Bit(10L, 4L, Map.empty, Map("height" -> 32.0))
      )
    }
    "execute it successfully with max aggregation with ordering" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationLongMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(MaxAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height"))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0L, 3L, Map.empty, Map("height" -> 30.5)),
        Bit(0L, 5L, Map.empty, Map("height" -> 31.0)),
        Bit(0L, 4L, Map.empty, Map("height" -> 32.0))
      )
    }
    "execute it successfully with min aggregation with ordering" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationLongMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(MinAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height"))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0L, 2L, Map.empty, Map("height" -> 30.5)),
        Bit(0L, 5L, Map.empty, Map("height" -> 31.0)),
        Bit(0L, 1L, Map.empty, Map("height" -> 32.0))
      )
    }

    "execute it successfully with max aggregation with ordering and limiting" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationLongMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(MaxAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height")),
            limit = Some(LimitOperator(2))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0L, 3L, Map.empty, Map("height" -> 30.5)),
        Bit(0L, 5L, Map.empty, Map("height" -> 31.0))
      )
    }
    "execute it successfully with min aggregation with ordering and limiting" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationLongMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(MinAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height")),
            limit = Some(LimitOperator(2))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0L, 2L, Map.empty, Map("height" -> 30.5)),
        Bit(0L, 5L, Map.empty, Map("height" -> 31.0))
      )
    }
    "execute it successfully with avg aggregation on a long value metric" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationLongMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(AvgAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height")),
            limit = None
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0L, 2.5, Map.empty, Map("height" -> 30.5)),
        Bit(0L, 5.0, Map.empty, Map("height" -> 31.0)),
        Bit(0L, 2.0, Map.empty, Map("height" -> 32.0))
      )
    }
    "execute it successfully with avg aggregation on a double value metric" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationDoubleMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(AvgAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("height")),
            order = Some(AscOrderOperator("height")),
            limit = None
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0L, 2.5, Map.empty, Map("height" -> 30.5)),
        Bit(0L, 5.0, Map.empty, Map("height" -> 31.0)),
        Bit(0L, 2.0, Map.empty, Map("height" -> 32.0))
      )
    }
  }
}
