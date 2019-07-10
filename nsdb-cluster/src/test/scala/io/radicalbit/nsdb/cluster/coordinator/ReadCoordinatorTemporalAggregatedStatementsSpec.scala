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
import org.scalactic.TolerantNumerics

import scala.concurrent.Await
import scala.concurrent.duration._

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

    expectNoMessage(interval)
  }

  "ReadCoordinator" when {

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
              ) //, startTime
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 10

        implicit val longEq = TolerantNumerics.tolerantLongEquality(System.currentTimeMillis - startTime)

        (0 to 9).foreach { i =>
          assert(expected.values(i).timestamp === startTime - (10 - i) * 30000)
          expected.values(i).dimensions("lowerBound") === startTime - (10 - i) * 30000
          expected.values(i).dimensions("upperBound") === startTime - (9 - i) * 30000
        }

        expected.values.map(_.value) shouldBe Seq(0, 0, 0, 0, 1, 1, 1, 1, 1, 1)

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
                groupBy = Some(TemporalGroupByAggregation(60000))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 10

        implicit val longEq = TolerantNumerics.tolerantLongEquality(System.currentTimeMillis - startTime)

        (0 to 9).foreach { i =>
          assert(expected.values(i).timestamp === startTime - (10 - i) * 60000)
          expected.values(i).dimensions("lowerBound") === startTime - (10 - i) * 60000
          expected.values(i).dimensions("upperBound") === startTime - (9 - i) * 60000
        }

        expected.values.map(_.value) shouldBe Seq(0, 0, 0, 0, 0, 0, 0, 2, 2, 2)
      }

    }

    "receive a select containing a timestamp where condition and a temporal group by" should {
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
                                                 value = startTime - 60000))),
                groupBy = Some(TemporalGroupByAggregation(30000))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 3

        implicit val longEq = TolerantNumerics.tolerantLongEquality(System.currentTimeMillis - startTime)

        (0 to 2).foreach { i =>
          assert(expected.values(i).timestamp === startTime - (3 - i) * 30000)
          expected.values(i).dimensions("lowerBound") === startTime - (3 - i) * 30000
          expected.values(i).dimensions("upperBound") === startTime - (2 - i) * 30000
        }

        expected.values.map(_.value) shouldBe Seq(1, 1, 1)
      }

      /*"execute it successfully in case of LT" in within(5.seconds) {
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
                                                 value = startTime - 60000))),
                groupBy = Some(TemporalGroupByAggregation(30000))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.size shouldBe 10

        implicit val longEq = TolerantNumerics.tolerantLongEquality(System.currentTimeMillis - startTime)

        println(expected.values)

        (0 to 2).foreach { i =>
          assert(expected.values(i).timestamp === startTime - (3 - i) * 30000)
          expected.values(i).dimensions("lowerBound") === startTime - (3 - i) * 30000
          expected.values(i).dimensions("upperBound") === startTime - (2 - i) * 30000
        }

        expected.values.map(_.value) shouldBe Seq(1, 1, 1)
      }*/
    }
  }
}
