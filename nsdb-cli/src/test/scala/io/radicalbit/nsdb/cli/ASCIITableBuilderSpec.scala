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

package io.radicalbit.nsdb.cli

import io.radicalbit.nsdb.cli.table.ASCIITableBuilder
import io.radicalbit.nsdb.common.protocol.{Bit, SQLStatementExecuted}
import io.radicalbit.nsdb.test.NSDbSpec

import scala.util.Success

class ASCIITableBuilderSpec extends NSDbSpec {

  def statementFor(res: Seq[Bit]) = SQLStatementExecuted(db = "db", namespace = "registry", metric = "people", res)

  "A parser instance" when {

    "receive a select projecting a wildcard" should {
      "parse it successfully" in {

        val input = List(
          Bit(timestamp = 1L,
              value = 10,
              dimensions = Map("name" -> "Roger", "surname" -> "Sterling", "age" -> 65),
              Map.empty),
          Bit(timestamp = 2L, value = 20, dimensions = Map("name"    -> "Don", "surname" -> "Draper"), Map.empty),
          Bit(timestamp = 3L, value = 30, dimensions = Map("age"     -> 28, "surname" -> "Olson"), Map.empty),
          Bit(timestamp = 4L, value = 40, dimensions = Map("name"    -> "Pete"), Map.empty),
          Bit(timestamp = 5L, value = 50, dimensions = Map("age"     -> "32"), Map.empty),
          Bit(timestamp = 6L, value = 60, dimensions = Map("surname" -> "Holloway"), Map.empty)
        )

        val expected = """+-----------------------+-----+---+-----+--------+
          ?|timestamp              |value|age|name |surname |
          ?+-----------------------+-----+---+-----+--------+
          ?|1970-01-01T00:00:00.001|10   |65 |Roger|Sterling|
          ?+-----------------------+-----+---+-----+--------+
          ?|1970-01-01T00:00:00.002|20   |   |Don  |Draper  |
          ?+-----------------------+-----+---+-----+--------+
          ?|1970-01-01T00:00:00.003|30   |28 |     |Olson   |
          ?+-----------------------+-----+---+-----+--------+
          ?|1970-01-01T00:00:00.004|40   |   |Pete |        |
          ?+-----------------------+-----+---+-----+--------+
          ?|1970-01-01T00:00:00.005|50   |32 |     |        |
          ?+-----------------------+-----+---+-----+--------+
          ?|1970-01-01T00:00:00.006|60   |   |     |Holloway|
          ?+-----------------------+-----+---+-----+--------+""".stripMargin('?')

        val tableBuilder = new ASCIITableBuilder(100)
        tableBuilder.tableFor(statementFor(input)) shouldBe Success(expected)
      }
    }
  }
}
