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

import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsAdded
import io.radicalbit.nsdb.cluster.coordinator.mockedData.MockedData._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  MetricDropped,
  RecordAccumulated,
  SchemaUpdated,
  SelectStatementExecuted
}

class ReadCoordinatorCharsetSpec extends AbstractReadCoordinatorSpec {

  override def prepareTestData(): Unit = {
    val location1 = Location(_: String, node, 100000, 190000)
    val location2 = Location(_: String, node, 0, 90000)

    //drop metrics
    probe.send(metricsDataActor,
               DropMetricWithLocations(db,
                                       namespace,
                                       CharsetMetric.name,
                                       Seq(location1(CharsetMetric.name), location2(CharsetMetric.name))))
    probe.expectMsgType[MetricDropped]

    probe.send(schemaCoordinator,
               UpdateSchemaFromRecord(db, namespace, CharsetMetric.name, CharsetMetric.testRecords.head))
    probe.expectMsgType[SchemaUpdated]

    probe.send(metadataCoordinator,
               AddLocations(db, namespace, Seq(location1(CharsetMetric.name), location2(CharsetMetric.name))))
    probe.expectMsgType[LocationsAdded]

    CharsetMetric.recordsShard1
      .foreach(r => {
        probe.send(metricsDataActor, AddRecordToShard(db, namespace, location1(CharsetMetric.name), r))
        probe.expectMsgType[RecordAccumulated]
      })
    CharsetMetric.recordsShard2
      .foreach(r => {
        probe.send(metricsDataActor, AddRecordToShard(db, namespace, location2(CharsetMetric.name), r))
        probe.expectMsgType[RecordAccumulated]
      })

  }

  "ReadCoordinator" when {

    "receive a select containing an equality expression with special characters on a tag" should {
      "execute it successfully if the comparison term has got no spaces" in {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = CharsetMetric.name,
                distinct = false,
                fields = AllFields(),
                condition = Some(
                  Condition(EqualityExpression(dimension = "textTag", value = AbsoluteComparisonValue("a_:m?!-e"))))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(60000, 7L, Map("textDimension" -> "a_:m?!-e"), Map("textTag" -> "a_:m?!-e"))
        )

      }

      "execute it successfully if the comparison term has got spaces" in {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = CharsetMetric.name,
                distinct = false,
                fields = AllFields(),
                condition = Some(
                  Condition(
                    EqualityExpression(dimension = "textTag",
                                       value = AbsoluteComparisonValue("Is this an empathized question?! Or not?"))))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(120000,
              3L,
              Map("textDimension" -> "Is this an empathized question?! Or not?"),
              Map("textTag"       -> "Is this an empathized question?! Or not?"))
        )

      }
    }

    "receive a select containing an equality expression with special characters on a dimension" should {
      "execute it successfully if the comparison term has got no spaces" in {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = CharsetMetric.name,
                distinct = false,
                fields = AllFields(),
                condition = Some(Condition(
                  EqualityExpression(dimension = "textDimension", value = AbsoluteComparisonValue("a_:m?!-e"))))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(60000, 7L, Map("textDimension" -> "a_:m?!-e"), Map("textTag" -> "a_:m?!-e"))
        )

      }

      "execute it successfully if the comparison term has got spaces" in {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = CharsetMetric.name,
                distinct = false,
                fields = AllFields(),
                condition = Some(
                  Condition(
                    EqualityExpression(dimension = "textDimension",
                                       value = AbsoluteComparisonValue("Is this an empathized question?! Or not?"))))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values shouldBe Seq(
          Bit(120000,
              3L,
              Map("textDimension" -> "Is this an empathized question?! Or not?"),
              Map("textTag"       -> "Is this an empathized question?! Or not?"))
        )

      }
    }

    "receive a select containing a like expression with special characters on a tag" should {
      "execute it successfully in case of a single condition" in {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = CharsetMetric.name,
                distinct = false,
                fields = AllFields(),
                condition = Some(Condition(LikeExpression(dimension = "textTag", value = "$?$")))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.sortBy(_.timestamp) shouldBe Seq(
          Bit(60000L, 7L, Map("textDimension" -> "a_:m?!-e"), Map("textTag" -> "a_:m?!-e")),
          Bit(90000,
              5L,
              Map("textDimension" -> "Is this a double question??"),
              Map("textTag"       -> "Is this a double question??")),
          Bit(120000L,
              3L,
              Map("textDimension" -> "Is this an empathized question?! Or not?"),
              Map("textTag"       -> "Is this an empathized question?! Or not?")),
          Bit(150000,
              2L,
              Map("textDimension" -> "Is this a question? Really"),
              Map("textTag"       -> "Is this a question? Really"))
        )

      }

      "execute it successfully in case of a tupled condition" in {

        val expected = awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = CharsetMetric.name,
                distinct = false,
                fields = AllFields(),
                condition = Some(
                  Condition(
                    TupledLogicalExpression(
                      LikeExpression(dimension = "textTag", value = "$?$"),
                      AndOperator,
                      NotExpression(LikeExpression(dimension = "textTag", value = "$?!$"))
                    )))
              )
            )
          )

          probe.expectMsgType[SelectStatementExecuted]
        }

        expected.values.sortBy(_.timestamp) shouldBe Seq(
          Bit(90000,
              5L,
              Map("textDimension" -> "Is this a double question??"),
              Map("textTag"       -> "Is this a double question??")),
          Bit(150000,
              2L,
              Map("textDimension" -> "Is this a question? Really"),
              Map("textTag"       -> "Is this a question? Really"))
        )

      }
    }
  }

}
