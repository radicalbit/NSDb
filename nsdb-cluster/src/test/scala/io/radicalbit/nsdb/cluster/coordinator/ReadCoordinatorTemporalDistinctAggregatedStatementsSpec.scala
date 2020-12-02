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

class ReadCoordinatorTemporalDistinctAggregatedStatementsSpec extends AbstractTemporalReadCoordinatorSpec {

  "ReadCoordinator" when {

    "receive a select containing a temporal group by with count distinct aggregation" should {
      "execute it successfully when count( distinct *) is used instead of value" in {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              selectStatement = SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(CountDistinctAggregation("value"))))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(10000, 1L, Map("lowerBound"  -> 0L, "upperBound"      -> 10000L), Map()),
          Bit(40000, 1L, Map("lowerBound"  -> 10000L, "upperBound"  -> 40000L), Map()),
          Bit(70000, 1L, Map("lowerBound"  -> 40000L, "upperBound"  -> 70000L), Map()),
          Bit(100000, 1L, Map("lowerBound" -> 70000L, "upperBound"  -> 100000L), Map()),
          Bit(130000, 1L, Map("lowerBound" -> 100000L, "upperBound" -> 130000L), Map()),
          Bit(160000, 1L, Map("lowerBound" -> 130000L, "upperBound" -> 160000L), Map())
        )

      }

      "execute it successfully when no shard has picked up" in {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              selectStatement = SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                condition = Some(
                  Condition(ComparisonExpression(dimension = "timestamp",
                                                 comparison = GreaterOrEqualToOperator,
                                                 value = AbsoluteComparisonValue(200000L)))),
                fields = ListFields(List(Field("*", Some(CountDistinctAggregation("value"))))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s")),
                limit = Some(LimitOperator(2))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 0
      }

      "execute it successfully when time ranges contain more than one distinct value" in {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              selectStatement = SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(10000, 1L, Map("lowerBound"  -> 0L, "upperBound"      -> 10000L), Map()),
          Bit(70000, 2L, Map("lowerBound"  -> 10000L, "upperBound"  -> 70000L), Map()),
          Bit(130000, 2L, Map("lowerBound" -> 70000L, "upperBound"  -> 130000L), Map()),
          Bit(190000, 1L, Map("lowerBound" -> 130000L, "upperBound" -> 190000L), Map())
        )
      }

    }

    "receive a select containing a temporal group by with count distinct aggregation on a double value metric" should {
      "execute it successfully when count(distinct *) is used instead of value" in {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              selectStatement = SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(CountDistinctAggregation("value"))))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(10000, 1L, Map("lowerBound"  -> 0L, "upperBound"      -> 10000L), Map()),
          Bit(40000, 1L, Map("lowerBound"  -> 10000L, "upperBound"  -> 40000L), Map()),
          Bit(70000, 1L, Map("lowerBound"  -> 40000L, "upperBound"  -> 70000L), Map()),
          Bit(100000, 1L, Map("lowerBound" -> 70000L, "upperBound"  -> 100000L), Map()),
          Bit(130000, 1L, Map("lowerBound" -> 100000L, "upperBound" -> 130000L), Map()),
          Bit(160000, 1L, Map("lowerBound" -> 130000L, "upperBound" -> 160000L), Map())
        )

      }

      "execute it successfully when time ranges contain more than one distinct value" in {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              selectStatement = SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(10000, 1L, Map("lowerBound"  -> 0L, "upperBound"      -> 10000L), Map()),
          Bit(70000, 2L, Map("lowerBound"  -> 10000L, "upperBound"  -> 70000L), Map()),
          Bit(130000, 2L, Map("lowerBound" -> 70000L, "upperBound"  -> 130000L), Map()),
          Bit(190000, 1L, Map("lowerBound" -> 130000L, "upperBound" -> 190000L), Map())
        )
      }

    }

    "receive a select containing a temporal group with count distinct aggregation by with an interval higher than the shard interval" should {
      "execute it successfully" in {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              selectStatement = SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountDistinctAggregation("value"))))),
                condition = None,
                groupBy = Some(TemporalGroupByAggregation(100000L, 100, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 2

        expected.values shouldBe Seq(
          Bit(90000, 4L, Map("lowerBound"  -> 0L, "upperBound"     -> 90000L), Map()),
          Bit(190000, 2L, Map("lowerBound" -> 90000L, "upperBound" -> 190000L), Map())
        )
      }

      "execute it successfully in case of a where condition" in {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              selectStatement = SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
                condition = Some(
                  Condition(ComparisonExpression(dimension = "timestamp",
                                                 comparison = GreaterOrEqualToOperator,
                                                 value = AbsoluteComparisonValue(60000L)))),
                groupBy = Some(TemporalGroupByAggregation(100000L, 100, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 2

        expected.values shouldBe Seq(
          Bit(90000, 2L, Map("lowerBound"  -> 60000L, "upperBound" -> 90000L), Map()),
          Bit(190000, 2L, Map("lowerBound" -> 90000L, "upperBound" -> 190000L), Map())
        )
      }
    }

  }
}
