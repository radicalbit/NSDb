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

import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.AddRecordToLocation
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocation
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.concurrent.Await
import scala.concurrent.duration._

object TemporalDoubleMetric {

  val name = "temporalDoubleMetric"

  val recordsShard1: Seq[Bit] = Seq(
    Bit(150000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
    Bit(120000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John"))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(90000, 5.5, Map("surname" -> "Doe"), Map("name" -> "John")),
    Bit(60000, 7.5, Map("surname" -> "Doe"), Map("name" -> "Bill")),
    Bit(30000, 4.5, Map("surname" -> "Doe"), Map("name" -> "Frank")),
    Bit(0, 1.5, Map("surname"     -> "Doe"), Map("name" -> "Frankie"))
  )

  val testRecords: Seq[Bit] = recordsShard1 ++ recordsShard2

}

object TemporalLongMetric {

  val name = "temporalLongMetric"

  val recordsShard1: Seq[Bit] = Seq(
    Bit(150000L, 2L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 15L, "height" -> 30.5)),
    Bit(120000L, 3L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 20L, "height" -> 30.5))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(90000L, 5L, Map("surname" -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
    Bit(60000L, 7L, Map("surname" -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
    Bit(30000L, 4L, Map("surname" -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
    Bit(0L, 1L, Map("surname"     -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
  )

  val testRecords: Seq[Bit] = recordsShard1 ++ recordsShard2
}

class ReadCoordinatorTemporalAggregatedStatementsSpec extends AbstractReadCoordinatorSpec {

  override def beforeAll = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(5.second)

    Await.result(readCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, "node1"), 10 seconds)

    val location1 = Location(_: String, "node1", 100000, 190000)
    val location2 = Location(_: String, "node1", 0, 90000)

    //drop metrics
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 TemporalLongMetric.name,
                                                 Seq(location1(TemporalLongMetric.name),
                                                     location2(TemporalLongMetric.name))),
      10 seconds
    )
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 TemporalDoubleMetric.name,
                                                 Seq(location1(TemporalDoubleMetric.name),
                                                     location2(TemporalDoubleMetric.name))),
      10 seconds
    )

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
                                                            namespace,
                                                            TemporalLongMetric.name,
                                                            TemporalLongMetric.testRecords.head),
                 10 seconds)
    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
                                                            namespace,
                                                            TemporalDoubleMetric.name,
                                                            TemporalDoubleMetric.testRecords.head),
                 10 seconds)

    Await.result(metadataCoordinator ? AddLocation(db, namespace, location1(TemporalLongMetric.name)), 10 seconds)
    Await.result(metadataCoordinator ? AddLocation(db, namespace, location1(TemporalDoubleMetric.name)), 10 seconds)

    TemporalLongMetric.recordsShard1
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(TemporalLongMetric.name)),
                     10 seconds)
      })
    TemporalDoubleMetric.recordsShard1
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(TemporalDoubleMetric.name)),
                     10 seconds)
      })

    Await.result(metadataCoordinator ? AddLocation(db, namespace, location2(TemporalLongMetric.name)), 10 seconds)
    Await.result(metadataCoordinator ? AddLocation(db, namespace, location2(TemporalDoubleMetric.name)), 10 seconds)

    TemporalLongMetric.recordsShard2
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(TemporalLongMetric.name)),
                     10 seconds)
      })
    TemporalDoubleMetric.recordsShard2
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(TemporalDoubleMetric.name)),
                     10 seconds)
      })

    expectNoMessage(indexingInterval)
  }

  "ReadCoordinator" when {

    "receive a select containing a temporal group by with count aggregation" should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(CountAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 7

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 1, Map("lowerBound"  -> 10000, "upperBound"  -> 40000), Map()),
          Bit(40000, 1, Map("lowerBound"  -> 40000, "upperBound"  -> 70000), Map()),
          Bit(70000, 1, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 1, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 1, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )

      }

      "execute it successfully when no shard has picked up" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                condition = Some(
                  Condition(ComparisonExpression(dimension = "timestamp",
                                                 comparison = GreaterOrEqualToOperator,
                                                 value = AbsoluteComparisonValue(200000L)))),
                fields = ListFields(List(Field("*", Some(CountAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s")),
                limit = Some(LimitOperator(2))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 0
      }

      "execute it successfully when only one shard has picked up" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                condition = Some(
                  Condition(ComparisonExpression(dimension = "timestamp",
                                                 comparison = GreaterOrEqualToOperator,
                                                 value = AbsoluteComparisonValue(100000L)))),
                fields = ListFields(List(Field("*", Some(CountAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s")),
                limit = Some(LimitOperator(2))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 2

        expected.values shouldBe Seq(
          Bit(130000, 1, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )
      }

      "execute it successfully with limit" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(CountAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s")),
                limit = Some(LimitOperator(2))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 2

        expected.values shouldBe Seq(
          Bit(130000, 1, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )
      }

      "execute it successfully when time ranges contain more than one value" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 4

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 2, Map("lowerBound"  -> 10000, "upperBound"  -> 70000), Map()),
          Bit(70000, 2, Map("lowerBound"  -> 70000, "upperBound"  -> 130000), Map()),
          Bit(130000, 1, Map("lowerBound" -> 130000, "upperBound" -> 190000), Map())
        )
      }

    }

    "receive a select containing a temporal group by with count aggregation on a double value metric" should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(CountAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 7

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 1, Map("lowerBound"  -> 10000, "upperBound"  -> 40000), Map()),
          Bit(40000, 1, Map("lowerBound"  -> 40000, "upperBound"  -> 70000), Map()),
          Bit(70000, 1, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 1, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 1, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )

      }

      "execute it successfully when time ranges contain more than one value" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 4

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 2, Map("lowerBound"  -> 10000, "upperBound"  -> 70000), Map()),
          Bit(70000, 2, Map("lowerBound"  -> 70000, "upperBound"  -> 130000), Map()),
          Bit(130000, 1, Map("lowerBound" -> 130000, "upperBound" -> 190000), Map())
        )
      }

    }

    "receive a select containing a timestamp where condition and a temporal group by with count aggregation" should {
      "execute it successfully in case of GTE" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                condition = Some(
                  Condition(ComparisonExpression(dimension = "timestamp",
                                                 comparison = GreaterOrEqualToOperator,
                                                 value = AbsoluteComparisonValue(60000L)))),
                groupBy = Some(TemporalGroupByAggregation(30000L, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 5

        expected.values shouldBe Seq(
          Bit(60000, 1, Map("lowerBound"  -> 60000, "upperBound"  -> 70000), Map()),
          Bit(70000, 1, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 1, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 1, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )
      }

      "execute it successfully in case of LT" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                condition = Some(
                  Condition(ComparisonExpression(dimension = "timestamp",
                                                 comparison = LessThanOperator,
                                                 value = AbsoluteComparisonValue(60000L)))),
                groupBy = Some(TemporalGroupByAggregation(30000L, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 2

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"     -> 0, "upperBound"     -> 29999), Map()),
          Bit(29999, 1, Map("lowerBound" -> 29999, "upperBound" -> 59999), Map())
        )
      }
    }

    "receive a select containing a temporal group with count aggregation by with an interval higher than the shard interval" should {
      "execute it successfully" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                condition = None,
                groupBy = Some(TemporalGroupByAggregation(100000L, 100, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 2

        expected.values shouldBe Seq(
          Bit(0, 4, Map("lowerBound"     -> 0, "upperBound"     -> 90000), Map()),
          Bit(90000, 2, Map("lowerBound" -> 90000, "upperBound" -> 190000), Map())
        )
      }

      "execute it successfully in case of a where condition" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
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
          Bit(60000, 2, Map("lowerBound" -> 60000, "upperBound" -> 90000), Map()),
          Bit(90000, 2, Map("lowerBound" -> 90000, "upperBound" -> 190000), Map())
        )
      }
    }

    "receive a select containing a temporal group by with sum aggregation" should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(SumAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 7

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 4, Map("lowerBound"  -> 10000, "upperBound"  -> 40000), Map()),
          Bit(40000, 7, Map("lowerBound"  -> 40000, "upperBound"  -> 70000), Map()),
          Bit(70000, 5, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 3, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 2, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )

      }

      "execute it successfully when time ranges contain more than one value" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 4

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 11, Map("lowerBound" -> 10000, "upperBound"  -> 70000), Map()),
          Bit(70000, 8, Map("lowerBound"  -> 70000, "upperBound"  -> 130000), Map()),
          Bit(130000, 2, Map("lowerBound" -> 130000, "upperBound" -> 190000), Map())
        )
      }

    }

    "receive a select containing a temporal group by with sum aggregation on a double value metric" should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(SumAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 7

        expected.values shouldBe Seq(
          Bit(0, 1.5, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 4.5, Map("lowerBound"  -> 10000, "upperBound"  -> 40000), Map()),
          Bit(40000, 7.5, Map("lowerBound"  -> 40000, "upperBound"  -> 70000), Map()),
          Bit(70000, 5.5, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 3.5, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 2.5, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0.0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )

      }

      "execute it successfully when time ranges contain more than one value" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 4

        expected.values shouldBe Seq(
          Bit(0, 1.5, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 12.0, Map("lowerBound" -> 10000, "upperBound"  -> 70000), Map()),
          Bit(70000, 9.0, Map("lowerBound"  -> 70000, "upperBound"  -> 130000), Map()),
          Bit(130000, 2.5, Map("lowerBound" -> 130000, "upperBound" -> 190000), Map())
        )
      }

    }

    "receive a select containing a temporal group by with max aggregation" should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(MaxAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 7

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 4, Map("lowerBound"  -> 10000, "upperBound"  -> 40000), Map()),
          Bit(40000, 7, Map("lowerBound"  -> 40000, "upperBound"  -> 70000), Map()),
          Bit(70000, 5, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 3, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 2, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )

      }

      "execute it successfully when time ranges contain more than one value" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(MaxAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 4

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 7, Map("lowerBound"  -> 10000, "upperBound"  -> 70000), Map()),
          Bit(70000, 5, Map("lowerBound"  -> 70000, "upperBound"  -> 130000), Map()),
          Bit(130000, 2, Map("lowerBound" -> 130000, "upperBound" -> 190000), Map())
        )
      }

    }

    "receive a select containing a temporal group by with max aggregation on a double metric" should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(MaxAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 7

        expected.values shouldBe Seq(
          Bit(0, 1.5, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 4.5, Map("lowerBound"  -> 10000, "upperBound"  -> 40000), Map()),
          Bit(40000, 7.5, Map("lowerBound"  -> 40000, "upperBound"  -> 70000), Map()),
          Bit(70000, 5.5, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 3.5, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 2.5, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0.0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )

      }

      "execute it successfully when time ranges contain more than one value" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(MaxAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 4

        expected.values shouldBe Seq(
          Bit(0, 1.5, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 7.5, Map("lowerBound"  -> 10000, "upperBound"  -> 70000), Map()),
          Bit(70000, 5.5, Map("lowerBound"  -> 70000, "upperBound"  -> 130000), Map()),
          Bit(130000, 2.5, Map("lowerBound" -> 130000, "upperBound" -> 190000), Map())
        )
      }

    }

    "receive a select containing a temporal group by with min aggregation" should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(MinAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 7

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 4, Map("lowerBound"  -> 10000, "upperBound"  -> 40000), Map()),
          Bit(40000, 7, Map("lowerBound"  -> 40000, "upperBound"  -> 70000), Map()),
          Bit(70000, 5, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 3, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 2, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )

      }

      "execute it successfully when time ranges contain more than one value" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(MinAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 4

        expected.values shouldBe Seq(
          Bit(0, 1, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 4, Map("lowerBound"  -> 10000, "upperBound"  -> 70000), Map()),
          Bit(70000, 3, Map("lowerBound"  -> 70000, "upperBound"  -> 130000), Map()),
          Bit(130000, 2, Map("lowerBound" -> 130000, "upperBound" -> 190000), Map())
        )
      }

    }

    "receive a select containing a temporal group by with min aggregation on a double metric" should {
      "execute it successfully when count(*) is used instead of value" in within(5.seconds) {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(MinAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(30000, 30, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 7

        expected.values shouldBe Seq(
          Bit(0, 1.5, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 4.5, Map("lowerBound"  -> 10000, "upperBound"  -> 40000), Map()),
          Bit(40000, 7.5, Map("lowerBound"  -> 40000, "upperBound"  -> 70000), Map()),
          Bit(70000, 5.5, Map("lowerBound"  -> 70000, "upperBound"  -> 100000), Map()),
          Bit(100000, 3.5, Map("lowerBound" -> 100000, "upperBound" -> 130000), Map()),
          Bit(130000, 2.5, Map("lowerBound" -> 130000, "upperBound" -> 160000), Map()),
          Bit(160000, 0.0, Map("lowerBound" -> 160000, "upperBound" -> 190000), Map())
        )

      }

      "execute it successfully when time ranges contain more than one value" in within(5.seconds) {
        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalDoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(MinAggregation)))),
                groupBy = Some(TemporalGroupByAggregation(60000, 60, "s"))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 4

        expected.values shouldBe Seq(
          Bit(0, 1.5, Map("lowerBound"      -> 0, "upperBound"      -> 10000), Map()),
          Bit(10000, 4.5, Map("lowerBound"  -> 10000, "upperBound"  -> 70000), Map()),
          Bit(70000, 3.5, Map("lowerBound"  -> 70000, "upperBound"  -> 130000), Map()),
          Bit(130000, 2.5, Map("lowerBound" -> 130000, "upperBound" -> 190000), Map())
        )
      }

    }

  }
}
