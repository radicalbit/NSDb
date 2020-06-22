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
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import org.scalatest.{Matchers, WordSpec}

class DeleteSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A SQL parser instance" when {

    "receive a delete without a where condition" should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "DELETE FROM people") shouldBe a[
          SqlStatementParserFailure]
      }
    }

    "receive a delete containing a range selection" should {
      "parse it successfully" in {
        val query = "delete FROM people WHERE timestamp IN (2,4)"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            DeleteSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              condition = Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))
            )
          ))
      }

      "parse it successfully ignoring case" in {
        val query = "delete FrOm people where timestamp in (2,4)"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            DeleteSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              condition = Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))
            )
          ))
      }
    }

    "receive a delete containing a GTE selection" should {
      val query = "DELETE FROM people WHERE timestamp >= 10"
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            DeleteSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              condition = Condition(
                ComparisonExpression(dimension = "timestamp",
                                     comparison = GreaterOrEqualToOperator,
                                     value = AbsoluteComparisonValue(10L)))
            )
          ))
      }
    }

    "receive a delete containing a GT AND a LTE selection" should {
      "parse it successfully" in {
        val query = "Delete FROM people WHERE timestamp > 2 AND timestamp <= 4"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            DeleteSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              condition = Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(2L)),
                operator = AndOperator,
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessOrEqualToOperator,
                                                   value = AbsoluteComparisonValue(4L))
              ))
            )
          ))
      }
    }

    "receive a delete containing a GTE OR a LT selection" should {
      "parse it successfully" in {
        val query = "DELETE FROM people WHERE NOT timestamp >= 2 OR timestamp < 4"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            DeleteSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              condition = Condition(
                NotExpression(
                  expression = TupledLogicalExpression(
                    expression1 = ComparisonExpression(dimension = "timestamp",
                                                       comparison = GreaterOrEqualToOperator,
                                                       value = AbsoluteComparisonValue(2L)),
                    operator = OrOperator,
                    expression2 = ComparisonExpression(dimension = "timestamp",
                                                       comparison = LessThanOperator,
                                                       value = AbsoluteComparisonValue(4L))
                  )
                ))
            )
          ))
      }
    }

    "receive random string sequences" should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "fkjdskjfdlsf") shouldBe a[SqlStatementParserFailure]
      }
    }

  }
}
