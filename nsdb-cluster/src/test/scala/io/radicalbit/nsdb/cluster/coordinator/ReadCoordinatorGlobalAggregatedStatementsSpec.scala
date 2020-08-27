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
import io.radicalbit.nsdb.common.{NSDbDoubleType, NSDbLongType}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

class ReadCoordinatorGlobalAggregatedStatementsSpec extends AbstractReadCoordinatorSpec {

  "ReadCoordinator" when {

    "receive a select containing only one single global aggregations" should {

      "execute it successfully with a count without a limit" in {
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
              )
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(0, 0L, Map.empty, Map("count(*)" -> NSDbLongType(6L)))
        )
      }

      "execute it successfully with a count and a limit" in {
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
          Bit(0, 0L, Map.empty, Map("count(*)" -> NSDbLongType(4L)))
        )
      }

      "execute it successfully with only an average" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(AvgAggregation)))
              ),
              limit = Some(LimitOperator(4))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(0, 0L, Map.empty, Map("avg(*)" -> NSDbDoubleType(3.5)))
        )
      }
    }

    "receive a select containing mixed global aggregations and plain fields" should {

      "execute it successfully with mixed count and plain fields" in {
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
          Bit(1L, 1L, Map.empty, Map("name"  -> "John", "count(*)"    -> 6L)),
          Bit(2L, 2L, Map.empty, Map("name"  -> "John", "count(*)"    -> 6L)),
          Bit(4L, 3L, Map.empty, Map("name"  -> "J", "count(*)"       -> 6L)),
          Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "count(*)"    -> 6L)),
          Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "count(*)"   -> 6L)),
          Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "count(*)" -> 6L))
        )
      }

      "execute it successfully with mixed average and plain fields" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("*", Some(AvgAggregation)), Field("name", None))),
              limit = Some(LimitOperator(6))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.sortBy(_.timestamp) shouldBe Seq(
          Bit(1L, 1L, Map.empty, Map("name"  -> "John", "avg(*)"    -> 3.5)),
          Bit(2L, 2L, Map.empty, Map("name"  -> "John", "avg(*)"    -> 3.5)),
          Bit(4L, 3L, Map.empty, Map("name"  -> "J", "avg(*)"       -> 3.5)),
          Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "avg(*)"    -> 3.5)),
          Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "avg(*)"   -> 3.5)),
          Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "avg(*)" -> 3.5))
        )
      }

      "execute it successfully with mixed count, average and plain fields" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(AvgAggregation)), Field("*", Some(CountAggregation)), Field("name", None))),
              limit = Some(LimitOperator(6))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.sortBy(_.timestamp) shouldBe Seq(
          Bit(1L, 1L, Map.empty, Map("name"  -> "John", "avg(*)"    -> 3.5, "count(*)" -> 6L)),
          Bit(2L, 2L, Map.empty, Map("name"  -> "John", "avg(*)"    -> 3.5, "count(*)" -> 6L)),
          Bit(4L, 3L, Map.empty, Map("name"  -> "J", "avg(*)"       -> 3.5, "count(*)" -> 6L)),
          Bit(6L, 4L, Map.empty, Map("name"  -> "Bill", "avg(*)"    -> 3.5, "count(*)" -> 6L)),
          Bit(8L, 5L, Map.empty, Map("name"  -> "Frank", "avg(*)"   -> 3.5, "count(*)" -> 6L)),
          Bit(10L, 6L, Map.empty, Map("name" -> "Frankie", "avg(*)" -> 3.5, "count(*)" -> 6L))
        )
      }

      "execute it successfully with mixed count, average and plain fields and a condition" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(AvgAggregation)), Field("*", Some(CountAggregation)), Field("name", None))),
              condition = Some(
                Condition(
                  RangeExpression(dimension = "timestamp",
                                  value1 = AbsoluteComparisonValue(2L),
                                  value2 = AbsoluteComparisonValue(4L)))),
              limit = Some(LimitOperator(6))
            )
          )
        )
        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.sortBy(_.timestamp) shouldBe Seq(
          Bit(2L, 2L, Map.empty, Map("name" -> "John", "avg(*)" -> 2.5, "count(*)" -> 2L)),
          Bit(4L, 3L, Map.empty, Map("name" -> "J", "avg(*)"    -> 2.5, "count(*)" -> 2L))
        )
      }

      "fail when a non global aggregation is provided" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(CountAggregation)), Field("surname", None), Field("value", Some(SumAggregation)))),
              limit = Some(LimitOperator(4))
            )
          )
        )
        awaitAssert {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }
  }
}
