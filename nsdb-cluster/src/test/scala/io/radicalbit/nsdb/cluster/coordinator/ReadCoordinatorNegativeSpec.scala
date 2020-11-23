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

import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocations
import io.radicalbit.nsdb.cluster.coordinator.mockedData.MockedData.NegativeMetric
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.concurrent.Await
import scala.concurrent.duration._

class ReadCoordinatorNegativeSpec extends AbstractReadCoordinatorSpec {

  override def prepareTestData()(implicit timeout: Timeout): Unit = {
    val location1 = Location(_: String, "node1", 0, 5)
    val location2 = Location(_: String, "node1", 6, 10)

    //negative double metric
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 NegativeMetric.name,
                                                 Seq(location1(NegativeMetric.name), location2(NegativeMetric.name))),
      10 seconds
    )

    Await.result(
      schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, NegativeMetric.name, NegativeMetric.testRecords.head),
      10 seconds)

    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location1(NegativeMetric.name))), 10 seconds)
    NegativeMetric.recordsShard1.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToShard(db, namespace, location1(NegativeMetric.name), r), 10 seconds)
    })
    Await.result(metadataCoordinator ? AddLocations(db, namespace, Seq(location2(NegativeMetric.name))), 10 seconds)
    NegativeMetric.recordsShard2.foreach(r => {
      Await.result(metricsDataActor ? AddRecordToShard(db, namespace, location2(NegativeMetric.name), r), 10 seconds)
    })

  }

  "ReadCoordinator" when {

    "receive a select projecting a wildcard with a limit and a ordering" should {
      "execute it successfully when ordered on a negative field" in {

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = NegativeMetric.name,
              distinct = false,
              fields = AllFields(),
              limit = Some(LimitOperator(5)),
              order = Some(DescOrderOperator("age"))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(5, -3.0, Map("surname" -> "Doe"), Map("age" -> 20L, "name"  -> "John", "height"  -> 30.5)),
          Bit(9, -1.0, Map("surname" -> "Doe"), Map("age" -> 18L, "name"  -> "Frank", "height" -> -32.0)),
          Bit(7, -6.0, Map("surname" -> "Doe"), Map("age" -> 17L, "name"  -> "Bill", "height"  -> -31.0)),
          Bit(3, -2.0, Map("surname" -> "Doe"), Map("age" -> 15L, "name"  -> "John", "height"  -> 30.5)),
          Bit(2, -2.0, Map("surname" -> "Doe"), Map("age" -> -15L, "name" -> "Bill", "height"  -> -30.5))
        )
      }

      "execute it successfully when ordered by value" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = NegativeMetric.name,
              distinct = false,
              fields = AllFields(),
              limit = Some(LimitOperator(2)),
              order = Some(AscOrderOperator("value"))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values shouldBe Seq(
          Bit(7, -6.0, Map("surname" -> "Doe"), Map("age" -> 17L, "name"  -> "Bill", "height" -> -31.0), Set()),
          Bit(6, -5.0, Map("surname" -> "Doe"), Map("age" -> -16L, "name" -> "John", "height" -> -31.0), Set())
        )
      }

    }

    "receive a select containing a GTE selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = NegativeMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(
                Condition(
                  ComparisonExpression(dimension = "timestamp",
                                       comparison = GreaterOrEqualToOperator,
                                       value = AbsoluteComparisonValue(10L)))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(10, -4.0, Map(), Map("name" -> "Frankie"))
        )
      }
    }

    "receive a select containing a GTE and a NOT selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = NegativeMetric.name,
              distinct = false,
              fields = AllFields(),
              condition = Some(
                Condition(
                  NotExpression(
                    ComparisonExpression(dimension = "age",
                                         comparison = GreaterOrEqualToOperator,
                                         value = AbsoluteComparisonValue(0L))
                  )))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.size shouldBe 5
      }
    }

    "receive a select containing a GTE expression on value" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = NegativeMetric.name,
              distinct = false,
              fields = AllFields(),
              condition = Some(
                Condition(
                  ComparisonExpression(dimension = "value",
                                       comparison = GreaterThanOperator,
                                       value = AbsoluteComparisonValue(-2.0))))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.sortBy(_.timestamp) shouldBe Seq(
          Bit(8L, -1.0, Map("surname" -> "Doe"), Map("name" -> "Frank", "age" -> -17L, "height" -> 32.0)),
          Bit(9L, -1.0, Map("surname" -> "Doe"), Map("name" -> "Frank", "age" -> 18L, "height"  -> -32.0))
        )
      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = NegativeMetric.name,
              distinct = false,
              fields = AllFields(),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "age",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(0)),
                operator = AndOperator,
                expression2 = ComparisonExpression(dimension = "height",
                                                   comparison = LessOrEqualToOperator,
                                                   value = AbsoluteComparisonValue(0.0))
              )))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.size shouldBe 2
      }
    }

    "receive a select containing a = selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = NegativeMetric.name,
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition =
                Some(Condition(EqualityExpression(dimension = "timestamp", value = AbsoluteComparisonValue(2L))))
            )
          )
        )

        val expected = awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }
        expected.values.size should be(1)
      }

    }

    "receive a select containing a standard aggregation" should {
      "execute it successfully in case of a sum" in {

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = NegativeMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0, -2.0, Map(), Map("name"  -> "Frank")),
          Bit(0, -4.0, Map(), Map("name"  -> "Frankie")),
          Bit(0, -8.0, Map(), Map("name"  -> "Bill")),
          Bit(0, -13.0, Map(), Map("name" -> "John"))
        )
      }

    }
    "execute it successfully in case of a min" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = NegativeMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(MinAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("age")),
            order = Some(DescOrderOperator("age"))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0, -3.0, Map(), Map("age" -> 20L)),
        Bit(0, -1.0, Map(), Map("age" -> 18L)),
        Bit(0, -6.0, Map(), Map("age" -> 17L)),
        Bit(0, -2.0, Map(), Map("age" -> 15L)),
        Bit(0, -2.0, Map(), Map("age" -> -15L)),
        Bit(0, -5.0, Map(), Map("age" -> -16L)),
        Bit(0, -1.0, Map(), Map("age" -> -17L)),
        Bit(0, -4.0, Map(), Map("age" -> -18L)),
        Bit(0, -3.0, Map(), Map("age" -> -20L))
      )
    }
    "execute it successfully in case of a max" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = NegativeMetric.name,
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
        Bit(0, -1.0, Map(), Map("height" -> -32.0)),
        Bit(0, -5.0, Map(), Map("height" -> -31.0)),
        Bit(0, -2.0, Map(), Map("height" -> -30.5)),
        Bit(0, -2.0, Map(), Map("height" -> 30.5)),
        Bit(0, -1.0, Map(), Map("height" -> 32.0))
      )
    }

    "execute it successfully in case of a avg" in {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = NegativeMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(AvgAggregation("value"))))),
            groupBy = Some(SimpleGroupByAggregation("name")),
            order = Some(AscOrderOperator("name"))
          )
        )
      )

      awaitAssert {
        probe.expectMsgType[SelectStatementExecuted]
      }.values shouldBe Seq(
        Bit(0, -4.0, Map(), Map("name"  -> "Bill")),
        Bit(0, -1.0, Map(), Map("name"  -> "Frank")),
        Bit(0, -4.0, Map(), Map("name"  -> "Frankie")),
        Bit(0, -3.25, Map(), Map("name" -> "John")),
      )
    }
  }
}
