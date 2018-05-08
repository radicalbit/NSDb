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

package io.radicalbit.nsdb.cli

import io.radicalbit.nsdb.cli.table.ASCIITableBuilder
import io.radicalbit.nsdb.common.protocol.{Bit, SQLStatementExecuted}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class ASCIITableBuilderSpec extends WordSpec with Matchers {

  def statementFor(res: Seq[Bit]) = SQLStatementExecuted(db = "db", namespace = "registry", metric = "people", res)

  "A parser instance" when {

    "receive a select projecting a wildcard" should {
      "parse it successfully" in {

        val input = List(
          Bit(timestamp = 1L, value = 10, dimensions = Map("name"    -> "Roger", "surname" -> "Sterling", "age" -> 65)),
          Bit(timestamp = 2L, value = 20, dimensions = Map("name"    -> "Don", "surname" -> "Draper")),
          Bit(timestamp = 3L, value = 30, dimensions = Map("age"     -> 28, "surname" -> "Olson")),
          Bit(timestamp = 4L, value = 40, dimensions = Map("name"    -> "Pete")),
          Bit(timestamp = 5L, value = 50, dimensions = Map("age"     -> "32")),
          Bit(timestamp = 6L, value = 60, dimensions = Map("surname" -> "Holloway"))
        )

        val expected = Success(
          "+---------+-----+---+-----+--------+\n|timestamp|value|age|name |surname |\n+---------+-----+---+-----+--------+\n|1        |10   |65 |Roger|Sterling|\n+---------+-----+---+-----+--------+\n|2        |20   |   |Don  |Draper  |\n+---------+-----+---+-----+--------+\n|3        |30   |28 |     |Olson   |\n+---------+-----+---+-----+--------+\n|4        |40   |   |Pete |        |\n+---------+-----+---+-----+--------+\n|5        |50   |32 |     |        |\n+---------+-----+---+-----+--------+\n|6        |60   |   |     |Holloway|\n+---------+-----+---+-----+--------+")

        val tableBuilder = new ASCIITableBuilder(100)
        tableBuilder.tableFor(statementFor(input)) shouldBe expected
      }
    }
  }
}
