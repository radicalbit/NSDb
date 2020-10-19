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

import scala.concurrent.duration._

class ReadCoordinatorDistinctAggregatedStatementsSpec extends AbstractReadCoordinatorSpec {

  "ReadCoordinator" when {

    "receive a select containing a count distinct and a group by on a string tag" should {
      "execute it successfully on a long metric" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountDistinctAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(0, 1L, Map(), Map("name" -> "Bill")),
          Bit(0, 1L, Map(), Map("name" -> "Frankie")),
          Bit(0, 1L, Map(), Map("name" -> "J")),
          Bit(0, 2L, Map(), Map("name" -> "John")),
          Bit(0, 1L, Map(), Map("name" -> "Frank"))
        )
      }

      "execute it successfully in case of a where condition" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountDistinctAggregation("value"))))),
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
        expected.values shouldBe Seq(
          Bit(0, 1L, Map(), Map("name" -> "Bill")),
          Bit(0, 1L, Map(), Map("name" -> "Frankie")),
          Bit(0, 1L, Map(), Map("name" -> "Frank")),
          Bit(0, 2L, Map(), Map("name" -> "John"))
        )
      }
    }

    "receive a select containing a count distinct and a group by on a long tag" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountDistinctAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("age")),
              order = Some(AscOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(Bit(0L, 1L, Map.empty, Map("age" -> 20L)), Bit(0L, 4L, Map.empty, Map("age" -> 15L)))
      }
    }

    "receive a select containing a count distinct and group by on a double tag" should {
      "execute it successfully" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountDistinctAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("height")),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0L, 2L, Map.empty, Map("height" -> 32.0)),
          Bit(0L, 2L, Map.empty, Map("height" -> 30.5)),
          Bit(0L, 1L, Map.empty, Map("height" -> 31.0))
        )
      }
    }

    "receive a select containing a count distinct on a tag " should {
      "execute it successfully on a long metric" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = AggregationLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("age", Some(CountDistinctAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(0, 1L, Map(), Map("name" -> "Bill")),
          Bit(0, 1L, Map(), Map("name" -> "Frankie")),
          Bit(0, 1L, Map(), Map("name" -> "Frank")),
          Bit(0, 2L, Map(), Map("name" -> "John"))
        )
      }
    }
  }
}
