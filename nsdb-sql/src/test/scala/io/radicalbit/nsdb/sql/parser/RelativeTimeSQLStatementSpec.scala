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

package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.statement._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.TryValues._
import org.scalatest.OptionValues._

class RelativeTimeSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  private val seconds = 1000L
  private val minutes = 60 * seconds
  private val hours   = 60 * minutes
  private val days    = 24 * hours

  private val timestampTolerance = 100L

  "A SQL parser instance" when {

    "receive a select with a relative timestamp value" should {

      "parse it successfully using relative time in simple where equality condition" in {
        val statement =
          parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp = now - 10s")
        statement.success.value.isInstanceOf[SelectSQLStatement] shouldBe true
        val now = System.currentTimeMillis()

        val selectSQLStatement = statement.success.value.asInstanceOf[SelectSQLStatement]
        val expression =
          selectSQLStatement.condition.value.expression.asInstanceOf[EqualityExpression[RelativeTimestampValue]]

        val firstTimestamp = expression.value

        firstTimestamp.timestamp shouldBe now - 10 * seconds +- timestampTolerance
        firstTimestamp.operator shouldBe "-"
        firstTimestamp.quantity shouldBe 10
        firstTimestamp.unitMeasure shouldBe "s"
      }

      "parse it successfully relative time in simple where comparison condition" in {
        val statement =
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE timestamp >= now - 10s")

        statement.success.value.isInstanceOf[SelectSQLStatement] shouldBe true
        val now = System.currentTimeMillis()

        val selectSQLStatement = statement.success.value.asInstanceOf[SelectSQLStatement]
        val expression =
          selectSQLStatement.condition.value.expression.asInstanceOf[ComparisonExpression[RelativeTimestampValue]]

        val firstTimestamp = expression.value

        firstTimestamp.timestamp shouldBe now - 10 * seconds +- timestampTolerance
        firstTimestamp.operator shouldBe "-"
        firstTimestamp.quantity shouldBe 10
        firstTimestamp.unitMeasure shouldBe "s"
      }

      "parse it successfully using relative time in complex where condition" in {

        val statement = parser.parse(db = "db",
                                     namespace = "registry",
                                     input =
                                       "SELECT name FROM people WHERE timestamp < now + 5s and timestamp > now - 8d")
        statement.success.value.isInstanceOf[SelectSQLStatement] shouldBe true
        val now = System.currentTimeMillis()

        val selectSQLStatement = statement.success.value.asInstanceOf[SelectSQLStatement]
        val expression         = selectSQLStatement.condition.value.expression.asInstanceOf[TupledLogicalExpression]

        val firstTimestamp  = expression.expression1.asInstanceOf[ComparisonExpression[RelativeTimestampValue]].value
        val secondTimestamp = expression.expression2.asInstanceOf[ComparisonExpression[RelativeTimestampValue]].value

        firstTimestamp.timestamp shouldBe now + 5 * seconds +- timestampTolerance
        firstTimestamp.operator shouldBe "+"
        firstTimestamp.quantity shouldBe 5
        firstTimestamp.unitMeasure shouldBe "s"

        secondTimestamp.timestamp shouldBe now - 8 * days +- timestampTolerance
        secondTimestamp.operator shouldBe "-"
        secondTimestamp.quantity shouldBe 8
        secondTimestamp.unitMeasure shouldBe "d"
      }

      "parse it successfully using relative time in very complex where condition" in {

        val statement = parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "SELECT name FROM people WHERE timestamp < now + 30d and timestamp > now - 2h AND timestamp = now + 4m")
        statement.success.value.isInstanceOf[SelectSQLStatement] shouldBe true
        val now = System.currentTimeMillis()

        val selectSQLStatement = statement.success.value.asInstanceOf[SelectSQLStatement]
        val expression         = selectSQLStatement.condition.value.expression.asInstanceOf[TupledLogicalExpression]

        val firstTimestamp   = expression.expression1.asInstanceOf[ComparisonExpression[RelativeTimestampValue]].value
        val secondExpression = expression.expression2.asInstanceOf[TupledLogicalExpression]
        val secondTimestamp =
          secondExpression.expression1.asInstanceOf[ComparisonExpression[RelativeTimestampValue]].value
        val thirdTimestamp = secondExpression.expression2.asInstanceOf[EqualityExpression[RelativeTimestampValue]].value

        firstTimestamp.timestamp shouldBe now + 30 * days +- timestampTolerance
        firstTimestamp.operator shouldBe "+"
        firstTimestamp.quantity shouldBe 30
        firstTimestamp.unitMeasure shouldBe "d"

        secondTimestamp.timestamp shouldBe now - 2 * hours +- timestampTolerance
        secondTimestamp.operator shouldBe "-"
        secondTimestamp.quantity shouldBe 2
        secondTimestamp.unitMeasure shouldBe "h"

        thirdTimestamp.timestamp shouldBe now + 4 * minutes +- timestampTolerance
        thirdTimestamp.operator shouldBe "+"
        thirdTimestamp.quantity shouldBe 4
        thirdTimestamp.unitMeasure shouldBe "m"
      }

      "parse it successfully using relative time in very complex where condition with brackets" in {

        val statement = parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "SELECT name FROM people WHERE (timestamp < now + 30d and timestamp > now - 2h) or timestamp = now + 4m")
        statement.success.value.isInstanceOf[SelectSQLStatement] shouldBe true
        val now = System.currentTimeMillis()

        val selectSQLStatement = statement.success.value.asInstanceOf[SelectSQLStatement]
        val expression         = selectSQLStatement.condition.value.expression.asInstanceOf[TupledLogicalExpression]

        val thirdTimestamp = expression.expression2.asInstanceOf[EqualityExpression[RelativeTimestampValue]].value

        val secondExpression = expression.expression1.asInstanceOf[TupledLogicalExpression]
        val firstTimestamp =
          secondExpression.expression1.asInstanceOf[ComparisonExpression[RelativeTimestampValue]].value
        val secondTimestamp =
          secondExpression.expression2.asInstanceOf[ComparisonExpression[RelativeTimestampValue]].value

        firstTimestamp.timestamp shouldBe now + 30 * days +- timestampTolerance
        firstTimestamp.operator shouldBe "+"
        firstTimestamp.quantity shouldBe 30
        firstTimestamp.unitMeasure shouldBe "d"

        secondTimestamp.timestamp shouldBe now - 2 * hours +- timestampTolerance
        secondTimestamp.operator shouldBe "-"
        secondTimestamp.quantity shouldBe 2
        secondTimestamp.unitMeasure shouldBe "h"

        thirdTimestamp.timestamp shouldBe now + 4 * minutes +- timestampTolerance
        thirdTimestamp.operator shouldBe "+"
        thirdTimestamp.quantity shouldBe 4
        thirdTimestamp.unitMeasure shouldBe "m"
      }

      "parse it successfully with a relative timestamp range condition" in {
        val statement = parser.parse(db = "db",
                                     namespace = "registry",
                                     input = "SELECT name FROM people WHERE timestamp IN (now - 2 s, now + 4 s)")

        statement.success.value.isInstanceOf[SelectSQLStatement] shouldBe true
        val now = System.currentTimeMillis()

        val selectSQLStatement = statement.success.value.asInstanceOf[SelectSQLStatement]
        val expression =
          selectSQLStatement.condition.value.expression.asInstanceOf[RangeExpression[RelativeTimestampValue]]

        val firstTimestamp  = expression.value1
        val secondTimestamp = expression.value2

        expression.dimension shouldBe "timestamp"
        firstTimestamp.timestamp shouldBe now - 2 * seconds +- timestampTolerance
        firstTimestamp.operator shouldBe "-"
        firstTimestamp.quantity shouldBe 2
        firstTimestamp.unitMeasure shouldBe "s"
        secondTimestamp.timestamp shouldBe now + 4 * seconds +- timestampTolerance
        secondTimestamp.operator shouldBe "+"
        secondTimestamp.quantity shouldBe 4
        secondTimestamp.unitMeasure shouldBe "s"
      }

      "parse it successfully with a relative timestamp range condition with unnecessary brackets" in {
        val statement = parser.parse(db = "db",
                                     namespace = "registry",
                                     input = "SELECT name FROM people WHERE (timestamp IN (now - 2 s, now + 4 s))")

        statement.success.value.isInstanceOf[SelectSQLStatement] shouldBe true
        val now = System.currentTimeMillis()

        val selectSQLStatement = statement.success.value.asInstanceOf[SelectSQLStatement]
        val expression =
          selectSQLStatement.condition.value.expression.asInstanceOf[RangeExpression[RelativeTimestampValue]]

        val firstTimestamp  = expression.value1
        val secondTimestamp = expression.value2

        expression.dimension shouldBe "timestamp"
        firstTimestamp.timestamp shouldBe now - 2 * seconds +- timestampTolerance
        firstTimestamp.operator shouldBe "-"
        firstTimestamp.quantity shouldBe 2
        firstTimestamp.unitMeasure shouldBe "s"
        secondTimestamp.timestamp shouldBe now + 4 * seconds +- timestampTolerance
        secondTimestamp.operator shouldBe "+"
        secondTimestamp.quantity shouldBe 4
        secondTimestamp.unitMeasure shouldBe "s"
      }

      "parse it successfully with a mixed relative/absolute timestamp range condition" in {
        val statement = parser.parse(db = "db",
                                     namespace = "registry",
                                     input = "SELECT name FROM people WHERE timestamp IN (now - 2 s, 5)")

        statement.success.value.isInstanceOf[SelectSQLStatement] shouldBe true
        val now = System.currentTimeMillis()

        val selectSQLStatement = statement.success.value.asInstanceOf[SelectSQLStatement]
        val expression =
          selectSQLStatement.condition.value.expression.asInstanceOf[RangeExpression[RelativeTimestampValue]]

        val firstTimestamp = expression.value1
        expression.dimension shouldBe "timestamp"
        firstTimestamp.timestamp shouldBe now - 2 * seconds +- timestampTolerance
        firstTimestamp.operator shouldBe "-"
        firstTimestamp.quantity shouldBe 2
        firstTimestamp.unitMeasure shouldBe "s"
        expression.value2 shouldBe 5
      }

    }

  }

}
