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

import io.radicalbit.nsdb.common.statement.SQLStatement._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import org.scalatest.Inside._
import org.scalatest.OptionValues._
import io.radicalbit.nsdb.test.NSDbSpec

class RelativeTimeSQLStatementSpec extends NSDbSpec {

  private val parser = new SQLStatementParser

  "A SQL parser instance" when {

    "receive a select with a relative timestamp value" should {

      "parse it successfully using relative time in simple where equality condition" in {
        inside(
          parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp = now - 10s")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case EqualityExpression(_, comparisonValue) =>
                comparisonValue shouldBe RelativeComparisonValue(minus, 10, "S")
            }
        }
      }

      "parse it successfully relative time in simple where comparison condition" in {
        inside(
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE timestamp >= now - 10 sec")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case ComparisonExpression(_, _, comparisonValue) =>
                comparisonValue shouldBe RelativeComparisonValue(minus, 10, "SEC")
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
                comparisonValue shouldBe RelativeComparisonValue(plus, 0L, "S")
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
                timestampComparison shouldBe RelativeComparisonValue(plus, 0L, "S")
                ageComparison shouldBe AbsoluteComparisonValue(18L)
            }
        }
      }

      "parse it successfully relative time in complex comparison condition (AND/OR)" in {
        inside(
          parser.parse(
            db = "db",
            namespace = "registry",
            input =
              "SELECT name FROM people WHERE timestamp < now and timestamp > now - 2 hour OR timestamp = now + 4min")
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
                firstTimestampComparison shouldBe RelativeComparisonValue(plus, 0L, "S")
                secondTimestampComparison shouldBe RelativeComparisonValue(minus, 2L, "HOUR")
                thirdTimestampComparison shouldBe RelativeComparisonValue(plus, 4L, "MIN")
            }
        }
      }

      "parse it successfully using relative time in a complex where condition with brackets" in {
        inside(
          parser.parse(
            db = "db",
            namespace = "registry",
            input =
              "SELECT name FROM people WHERE (timestamp < now + 30 day and timestamp > now - 2h) or timestamp = now + 4d")
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
                firstTimestampComparison shouldBe RelativeComparisonValue(plus, 30L, "DAY")
                secondTimestampComparison shouldBe RelativeComparisonValue(minus, 2L, "H")
                thirdTimestampComparison shouldBe RelativeComparisonValue(plus, 4L, "D")
            }
        }
      }

      "parse it successfully with a relative timestamp range condition" in {
        inside(
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE timestamp IN (now - 2 s, now + 4 second)")
        ) {
          case SqlStatementParserSuccess(_, selectSQLStatement: SelectSQLStatement) =>
            inside(selectSQLStatement.condition.value.expression) {
              case RangeExpression(_,
                                   firstAggregation: RelativeComparisonValue,
                                   secondAggregation: RelativeComparisonValue) =>
                firstAggregation shouldBe RelativeComparisonValue(minus, 2L, "S")
                secondAggregation shouldBe RelativeComparisonValue(plus, 4L, "SECOND")
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
                firstAggregation shouldBe RelativeComparisonValue(minus, 2L, "S")
                secondAggregation shouldBe RelativeComparisonValue(plus, 4L, "S")
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
                firstAggregation shouldBe RelativeComparisonValue(minus, 2L, "S")
                secondAggregation shouldBe AbsoluteComparisonValue(5L)
            }
        }
      }

    }

  }

}
