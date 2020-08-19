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

package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.sql.parser.SQLStatementParser._
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import org.scalatest.Inside._
import org.scalatest.OptionValues._
import org.scalatest.{Matchers, WordSpec}

class RelativeTimeSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  private val seconds = 1000L
  private val minutes = 60 * seconds
  private val hours   = 60 * minutes
  24 * hours

  "A SQL parser instance" when {

    "receive a select with a relative timestamp value" should {

      "parse it successfully using relative time in simple where equality condition" in {
        inside(
          parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp = now - 10s")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case EqualityExpression(_, comparisonValue) =>
                comparisonValue shouldBe RelativeComparisonValue(10000L, minus, 10, second)
            }
        }
      }

      "parse it successfully relative time in simple where comparison condition" in {
        inside(
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE timestamp >= now - 10s")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case ComparisonExpression(_, _, comparisonValue) =>
                comparisonValue shouldBe RelativeComparisonValue(10000L, minus, 10, second)
            }
        }
      }

      "parse it successfully relative time in simple where comparison condition (now)" in {
        inside(
          parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp < now")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case ComparisonExpression(_, _, comparisonValue) =>
                comparisonValue shouldBe RelativeComparisonValue(0L, plus, 0L, second)
            }
        }
      }

      "parse it successfully relative time with double comparison condition (AND)" in {
        inside(
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE timestamp < now AND age >= 18")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case TupledLogicalExpression(ComparisonExpression(_, _, timestampComparison),
                                           AndOperator,
                                           ComparisonExpression(_, _, ageComparison)) =>
                timestampComparison shouldBe RelativeComparisonValue(0L, plus, 0L, second)
                ageComparison shouldBe AbsoluteComparisonValue(18L)
            }
        }
      }

      "parse it successfully relative time in complex comparison condition (AND/OR)" in {
        inside(
          parser.parse(
            db = "db",
            namespace = "registry",
            input = "SELECT name FROM people WHERE timestamp < now and timestamp > now - 2h OR timestamp = now + 4m")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case TupledLogicalExpression(ComparisonExpression(_, _, firstTimestampComparison),
                                           AndOperator,
                                           TupledLogicalExpression(ComparisonExpression(_,
                                                                                        _,
                                                                                        secondTimestampComparison),
                                                                   OrOperator,
                                                                   EqualityExpression(_, thirdTimestampComparison))) =>
                firstTimestampComparison shouldBe RelativeComparisonValue(0L, plus, 0L, second)
                secondTimestampComparison shouldBe RelativeComparisonValue(7200000L, minus, 2L, hour)
                thirdTimestampComparison shouldBe RelativeComparisonValue(240000L, plus, 4L, minute)
            }
        }
      }

      "parse it successfully using relative time in a complex where condition with brackets" in {
        inside(
          parser.parse(
            db = "db",
            namespace = "registry",
            input =
              "SELECT name FROM people WHERE (timestamp < now + 30d and timestamp > now - 2h) or timestamp = now + 4d")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case TupledLogicalExpression(TupledLogicalExpression(ComparisonExpression(_, _, firstTimestampComparison),
                                                                   AndOperator,
                                                                   ComparisonExpression(_,
                                                                                        _,
                                                                                        secondTimestampComparison)),
                                           OrOperator,
                                           EqualityExpression(_, thirdTimestampComparison)) =>
                firstTimestampComparison shouldBe RelativeComparisonValue(2592000000L, plus, 30L, day)
                secondTimestampComparison shouldBe RelativeComparisonValue(7200000L, minus, 2L, hour)
                thirdTimestampComparison shouldBe RelativeComparisonValue(345600000L, plus, 4L, day)
            }
        }
      }

      "parse it successfully with a relative timestamp range condition" in {
        inside(
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE timestamp IN (now - 2 s, now + 4 s)")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case RangeExpression(_,
                                   firstAggregation: RelativeComparisonValue,
                                   secondAggregation: RelativeComparisonValue) =>
                firstAggregation shouldBe RelativeComparisonValue(2000L, minus, 2L, second)
                secondAggregation shouldBe RelativeComparisonValue(4000L, plus, 4L, second)
            }
        }
      }

      "parse it successfully with a relative timestamp range condition with unnecessary brackets" in {
        inside(
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE (timestamp IN (now - 2 s, now + 4 s))")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case RangeExpression(_,
                                   firstAggregation: RelativeComparisonValue,
                                   secondAggregation: RelativeComparisonValue) =>
                firstAggregation shouldBe RelativeComparisonValue(2000L, minus, 2L, second)
                secondAggregation shouldBe RelativeComparisonValue(4000L, plus, 4L, second)
            }
        }
      }

      "parse it successfully with a mixed relative/absolute timestamp range condition" in {
        inside(
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE timestamp IN (now - 2 s, 5)")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case RangeExpression(_,
                                   firstAggregation: RelativeComparisonValue,
                                   secondAggregation: AbsoluteComparisonValue) =>
                firstAggregation shouldBe RelativeComparisonValue(2000L, minus, 2L, second)
                secondAggregation shouldBe AbsoluteComparisonValue(5L)
            }
        }
      }

    }

  }

}
