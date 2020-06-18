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
import io.radicalbit.nsdb.sql.parser.StatementParserResult.SqlStatementParserSuccess
import org.scalatest._

class SelectSQLLikeExpressionSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A SQL parser instance" when {

    "receive a select containing a like selection" should {

      "parse it successfully" in {
        val query = "SELECT name FROM people WHERE name like $ame$"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(LikeExpression(dimension = "name", value = "$ame$")))
            )
          ))
      }

      "parse it successfully with predicate containing special characters" in {
        val query = "SELECT name FROM people WHERE name like $a_m-e$"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(LikeExpression(dimension = "name", value = "$a_m-e$")))
            )
          ))
      }

      "parse it successfully with predicate containing special characters and spaces" in {
        val query = "SELECT name FROM people WHERE name like '$a_m- e$'"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(LikeExpression(dimension = "name", value = "$a_m- e$")))
            )
          ))
      }

      "parse it successfully with predicate inside a bracket" in {
        val query = "SELECT name FROM people WHERE (name like $a_m-e$)"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(LikeExpression(dimension = "name", value = "$a_m-e$")))
            )
          ))
      }
    }
  }

}
