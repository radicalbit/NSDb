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
import akka.pattern.ask
import org.scalactic.TolerantNumerics

object TemporalDoubleMetric {

  val name = "temporalDoubleMetric"

  def recordsShard1(currentTime: Long): Seq[Bit] = Seq(
    Bit(currentTime, 1.5, Map("surname"         -> "Doe"), Map("name" -> "John")),
    Bit(currentTime - 30000, 1.5, Map("surname" -> "Doe"), Map("name" -> "John"))
  )

  def recordsShard2(currentTime: Long): Seq[Bit] = Seq(
    Bit(currentTime - 60000, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(currentTime - 90000, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(currentTime - 120000, 1.5, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
  )

  def testRecords(currentTime: Long): Seq[Bit] = recordsShard1(currentTime) ++ recordsShard2(currentTime)

}

object TemporalLongMetric {

  val name = "temporalLongMetric"

  def recordsShard1(currentTime: Long): Seq[Bit] = Seq(
    Bit(currentTime, 2L, Map("surname"         -> "Doe"), Map("name" -> "John", "age" -> 15L, "height" -> 30.5)),
    Bit(currentTime - 30000, 2L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 20L, "height" -> 30.5))
  )

  def recordsShard2(currentTime: Long): Seq[Bit] = Seq(
    Bit(currentTime - 60000, 1L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
    Bit(currentTime - 90000, 1L, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
    Bit(currentTime - 120000, 1L, Map("surname" -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
    Bit(currentTime - 150000, 1L, Map("surname" -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
  )

  def testRecords(currentTime: Long): Seq[Bit] = recordsShard1(currentTime) ++ recordsShard2(currentTime)
}

class ReadCoordinatorTemporalAggregatedStatementsSpec extends AbstractReadCoordinatorSpec {

  val startTime = System.currentTimeMillis()

  override def beforeAll = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(5.second)

    Await.result(readCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, "node1"), 10 seconds)

    val location1 = Location(_: String, "node1", startTime - 40000, startTime)
    val location2 = Location(_: String, "node1", startTime - 150000, startTime - 40000)

    //long metric
    Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
                                                 namespace,
                                                 TemporalLongMetric.name,
                                                 Seq(location1(LongMetric.name), location2(LongMetric.name))),
      10 seconds)

    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db,
                                                            namespace,
                                                            TemporalLongMetric.name,
                                                            TemporalLongMetric.testRecords(startTime).head),
                 10 seconds)

    Await.result(metadataCoordinator ? AddLocation(db, namespace, location1(TemporalLongMetric.name)), 10 seconds)
    TemporalLongMetric
      .recordsShard1(startTime)
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(TemporalLongMetric.name)),
                     10 seconds)
      })
    Await.result(metadataCoordinator ? AddLocation(db, namespace, location2(TemporalLongMetric.name)), 10 seconds)
    TemporalLongMetric
      .recordsShard2(startTime)
      .foreach(r => {
        Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(TemporalLongMetric.name)),
                     10 seconds)
      })

    //double metric
    /*Await.result(
      metricsDataActor ? DropMetricWithLocations(db,
        namespace,
        TemporalDoubleMetric.name,
        Seq(location1(DoubleMetric.name), location2(DoubleMetric.name))),
      10 seconds)

    Await.result(
      schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, DoubleMetric.name, DoubleMetric.testRecords.head),
      10 seconds)

    DoubleMetric.recordsShard1.foreach(r => {
      Await.result(metadataCoordinator ? AddLocation(db, namespace, location1(DoubleMetric.name)), 10 seconds)
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location1(DoubleMetric.name)), 10 seconds)
    })
    DoubleMetric.recordsShard2.foreach(r => {
      Await.result(metadataCoordinator ? AddLocation(db, namespace, location2(DoubleMetric.name)), 10 seconds)
      Await.result(metricsDataActor ? AddRecordToLocation(db, namespace, r, location2(DoubleMetric.name)), 10 seconds)
    })*/

//    expectNoMessage(interval)
    expectNoMessage(interval)
  }

  "ReadCoordinator" when {

    "receive a select containing a GTE selection and a temporal group by" should {
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
                condition = Some(Condition(
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
                groupBy = Some(TemporalGroupByAggregation(30000))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }
        println(expected.values.sortBy(_.timestamp))
        println(expected.values.filter(_.value.toString != "0"))
        expected.values.exists(_.value.toString != "0") shouldBe true
      }
    }

    "receive a select containing a temporal group by" should {
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
                groupBy = Some(TemporalGroupByAggregation(30000))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        println(expected.values)
        println(startTime)
        println(System.currentTimeMillis - startTime)
        println(expected.values.head)
        println(expected.values.last)
        println(expected.values.head.dimensions("upperBound").asInstanceOf[Long] - startTime)
        println(startTime - expected.values.head.dimensions("upperBound").asInstanceOf[Long])
//        println(expected.values.last.dimensions("upperBound").asInstanceOf[Long] - startTime)
//        println(startTime - expected.values.last.dimensions("upperBound").asInstanceOf[Long])
//        println(expected.values.last.timestamp - startTime)

        implicit val longEq = TolerantNumerics.tolerantLongEquality(System.currentTimeMillis - startTime + 1000)

//        assert(expected.values.head.dimensions("upperBound") === currentTime)
        assert(startTime === expected.values.head.dimensions("upperBound"))

        (0 to 9).foreach { i =>
          assert(expected.values(i).timestamp === startTime - (10 - i) * 30000)
//          expected.values(i).dimensions("lowerBound") === 15//currentTime - (10 - i) * 30000 +- 50L
//          expected.values(i).dimensions("upperBound") === currentTime - (9 - i) * 30000 +- 50L
        }

//        expected.values shouldBe Seq(
//          Bit(1562346870000L, 0, Map("lowerBound" -> 1562346870000L, "upperBound" -> 1562346900000L), Map()),
//          Bit(1562346780000L, 0, Map("lowerBound" -> 1562346780000L, "upperBound" -> 1562346810000L), Map()),
//          Bit(1562346900000L, 0, Map("lowerBound" -> 1562346900000L, "upperBound" -> 1562346930000L), Map()),
//          Bit(1562346690000L, 0, Map("lowerBound" -> 1562346690000L, "upperBound" -> 1562346720000L), Map()),
//          Bit(1562346930000L, 0, Map("lowerBound" -> 1562346930000L, "upperBound" -> 1562346960000L), Map()),
//          Bit(1562346720000L, 0, Map("lowerBound" -> 1562346720000L, "upperBound" -> 1562346750000L), Map()),
//          Bit(1562346960000L, 0, Map("lowerBound" -> 1562346960000L, "upperBound" -> 1562346990000L), Map()),
//          Bit(1562346840000L, 0, Map("lowerBound" -> 1562346840000L, "upperBound" -> 1562346870000L), Map()),
//          Bit(1562346810000L, 0, Map("lowerBound" -> 1562346810000L, "upperBound" -> 1562346840000L), Map()),
//          Bit(1562346750000L, 0, Map("lowerBound" -> 1562346750000L, "upperBound" -> 1562346780000L), Map())
//        )

        expected.values.reverse.zipWithIndex.foreach {
          case (v, i) =>
            v.timestamp === startTime - i * 30000 +- 1000L
            v.value === 1

        }

      }

      "execute it successfully with asc ordering over string dimension" in within(5.seconds) {
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
                groupBy = Some(TemporalGroupByAggregation(30000)),
                order = Some(AscOrderOperator("name"))
              )
            )
          )

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
              metric = TemporalLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some(TemporalGroupByAggregation(30000)),
              order = Some(DescOrderOperator("name"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values shouldBe Seq(
          Bit(0L, 3, Map.empty, Map("name" -> "John")),
          Bit(0L, 3, Map.empty, Map("name" -> "J")),
          Bit(0L, 6, Map.empty, Map("name" -> "Frankie")),
          Bit(0L, 5, Map.empty, Map("name" -> "Frank")),
          Bit(0L, 4, Map.empty, Map("name" -> "Bill"))
        )

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = TemporalDoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
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

      "execute it successfully with desc ordering over numerical dimension" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = TemporalLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some(TemporalGroupByAggregation(30000)),
              order = Some(DescOrderOperator("value"))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value) shouldBe Seq(6, 5, 4, 3, 3)

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some(SimpleGroupByAggregation("name")),
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
              metric = TemporalLongMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some(TemporalGroupByAggregation(30000)),
              order = Some(AscOrderOperator("value")),
              limit = Some(LimitOperator(2))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementExecuted]
        }.values.map(_.value) shouldBe Seq(3, 3)

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = DoubleMetric.name,
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              groupBy = Some(SimpleGroupByAggregation("name")),
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

    "receive a select containing a temporal group by //FIXME" should {
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
              groupBy = Some(TemporalGroupByAggregation(30000)),
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
              groupBy = Some(TemporalGroupByAggregation(30000)),
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
              groupBy = Some(TemporalGroupByAggregation(30000)),
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

    //FIXME sum aggregations are not supported yet
    "execute it successfully with sum aggregation" ignore {
      probe.send(
        readCoordinatorActor,
        ExecuteStatement(
          SelectSQLStatement(
            db = db,
            namespace = namespace,
            metric = AggregationMetric.name,
            distinct = false,
            fields = ListFields(List(Field("value", Some(SumAggregation)))),
            groupBy = Some(TemporalGroupByAggregation(30000)),
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
