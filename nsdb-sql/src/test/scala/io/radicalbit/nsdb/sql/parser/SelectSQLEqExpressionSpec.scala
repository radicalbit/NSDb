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
import io.radicalbit.nsdb.sql.parser.StatementParserResult.{SqlStatementParserFailure, SqlStatementParserSuccess}
import org.scalatest.{Matchers, WordSpec}

class SelectSQLEqExpressionSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A SQL parser instance" when {

    "receive a select containing a = selection" should {
      "parse it successfully" in {
        val query = "SELECT name FROM people WHERE timestamp = 10"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition =
                Some(Condition(EqualityExpression(dimension = "timestamp", value = AbsoluteComparisonValue(10L))))
            )
          ))
      }

      "parse it successfully using decimals" in {
        val query = "SELECT name FROM people WHERE timestamp = 10.5"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition =
                Some(Condition(EqualityExpression(dimension = "timestamp", value = AbsoluteComparisonValue(10.5))))
            )
          ))
      }

      "parse it successfully using string" in {
        val query = "SELECT name FROM people WHERE timestamp = word_word"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(
                Condition(EqualityExpression(dimension = "timestamp", value = AbsoluteComparisonValue("word_word"))))
            )
          ))
      }

      "parse it successfully on string dimension with spaces" in {
        val query = "select name from people where name = 'string spaced' limit 5"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(
                Condition(EqualityExpression(dimension = "name", value = AbsoluteComparisonValue("string spaced")))),
              limit = Some(LimitOperator(5))
            )
          ))
      }

      "parse it successfully on string dimension with special characters" in {
        val query = "select name from people where name = '_spe.cial~:_-' limit 5"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(
                Condition(EqualityExpression(dimension = "name", value = AbsoluteComparisonValue("_spe.cial~:_-")))),
              limit = Some(LimitOperator(5))
            )
          ))
      }

      "parse it successfully on string dimension with special characters and spaces" in {
        val query = "select name from people where name = '_spe~cial:_- with. space' limit 5"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(
                EqualityExpression(dimension = "name", value = AbsoluteComparisonValue("_spe~cial:_- with. space")))),
              limit = Some(LimitOperator(5))
            )
          ))
      }

      "parse it successfully on string dimension with one char" in {
        val query = "select name from people where name = 'a' limit 5"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(EqualityExpression(dimension = "name", value = AbsoluteComparisonValue("a")))),
              limit = Some(LimitOperator(5))
            )
          ))
      }

      "not allow wildcard char" in {
        val query = "select name from people where name = 'a$' limit 5"
        parser.parse(db = "db", namespace = "registry", input = query) shouldBe a[SqlStatementParserFailure]
      }
    }
  }
}
