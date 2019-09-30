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

import scala.util.Success

class SQLStatementBracketsSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A SQL parser instance" when {

    "receive a select containing a range selection inside square brackets" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE (timestamp IN (2,4))") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L)))
          )))
      }

      "parse it successfully using decimal values" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE (timestamp IN (2, 3.5))") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 3.5)))
          )))
      }

      "parse it successfully using relative time" in {
        val statement = parser.parse(db = "db",
                                     namespace = "registry",
                                     input = "SELECT name FROM people WHERE (timestamp IN (now - 2 s, now + 4 s))")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a = selection" should {
      "parse it successfully using string" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE (timestamp = word_word)") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(EqualityExpression(dimension = "timestamp", value = "word_word")))
          )))
      }

      "parse it successfully using relative time" in {
        val statement =
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE (timestamp = now - 10s)")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a like selection" should {
      "parse it successfully with predicate containing special characters" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE (name like $a_m-e$)") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(LikeExpression(dimension = "name", value = "$a_m-e$")))
          )))
      }
    }

    "receive a select containing a GTE selection" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE (timestamp >= 10)") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L)))
          )))
      }

      "parse it successfully using relative time" in {
        val statement =
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE (timestamp >= now - 10s)")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a GT AND a = selection" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE (timestamp > 2) AND (timestamp = 4)") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = AndOperator,
              expression2 = EqualityExpression(dimension = "timestamp", value = 4L)
            )))
          )))
      }

      "parse it successfully using relative time" in {
        val statement = parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "SELECT name FROM people WHERE timestamp < now + 30d and (timestamp > now - 2h) AND (timestamp = now + 4m)")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a NOT and a OR expression" should {
      "parse it successfully if not is applied to the or expression" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE NOT (timestamp >= 2 OR timestamp < 4)") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(UnaryLogicalExpression(
              expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
              ),
              operator = NotOperator
            )))
          )))
      }
      "parse it successfully if not is applied only to the first expression" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE (NOT timestamp >= 2) OR (timestamp < 4)") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 = UnaryLogicalExpression(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = NotOperator),
              operator = OrOperator,
              expression2 = ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
            )))
          )))
      }

      "parse it successfully if not is applied both to the whole chain and to an inner condition" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE NOT (timestamp >= 2 OR NOT timestamp < 4)") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(UnaryLogicalExpression(
              expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = UnaryLogicalExpression(
                  ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L),
                  operator = NotOperator)
              ),
              operator = NotOperator
            )))
          )))
      }
    }

    "receive a complex select containing 3 conditions a desc ordering statement and a limit statement" should {
      "parse it successfully when the first 2 expression are in brackets" in {
        parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "SELECT name FROM people WHERE (name like $an$ and surname = pippo) and timestamp IN (2,4)  ORDER BY name DESC LIMIT 5"
        ) should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 = TupledLogicalExpression(expression1 = LikeExpression("name", "$an$"),
                                                    operator = AndOperator,
                                                    expression2 = EqualityExpression("surname", "pippo")),
              operator = AndOperator,
              expression2 = RangeExpression("timestamp", 2, 4)
            ))),
            order = Some(DescOrderOperator(dimension = "name")),
            limit = Some(LimitOperator(5))
          )))
      }

      "parse it successfully when the last 2 expression are in brackets" in {
        parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "SELECT name FROM people WHERE name like $an$ and (surname = pippo and timestamp IN (2,4))  ORDER BY name DESC LIMIT 5"
        ) should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(
              Condition(TupledLogicalExpression(LikeExpression("name", "$an$"),
                                                AndOperator,
                                                TupledLogicalExpression(EqualityExpression("surname", "pippo"),
                                                                        AndOperator,
                                                                        RangeExpression("timestamp", 2, 4))))),
            order = Some(DescOrderOperator(dimension = "name")),
            limit = Some(LimitOperator(5))
          )))
      }

      "receive a select containing a condition of not nullable" in {
        parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "select * from AreaOccupancy where (name=MeetingArea) and (name is not null) order by timestamp desc limit 1") shouldBe
          Success(
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "AreaOccupancy",
              distinct = false,
              fields = AllFields,
              condition = Some(
                Condition(TupledLogicalExpression(EqualityExpression("name", "MeetingArea"),
                                                  AndOperator,
                                                  UnaryLogicalExpression(NullableExpression("name"), NotOperator)))),
              order = Some(DescOrderOperator(dimension = "timestamp")),
              limit = Some(LimitOperator(1))
            ))
      }

      "receive a select containing two conditions of not nullable" in {
        parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "select * from AreaOccupancy where (name=MeetingArea and name is not null) or floor is not null order by timestamp desc limit 1"
        ) shouldBe
          Success(
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "AreaOccupancy",
              distinct = false,
              fields = AllFields,
              condition = Some(
                Condition(
                  TupledLogicalExpression(
                    expression1 =
                      TupledLogicalExpression(expression1 = EqualityExpression("name", "MeetingArea"),
                                              operator = AndOperator,
                                              expression2 =
                                                UnaryLogicalExpression(NullableExpression("name"), NotOperator)),
                    operator = OrOperator,
                    expression2 = UnaryLogicalExpression(NullableExpression("floor"), NotOperator)
                  )
                )),
              order = Some(DescOrderOperator(dimension = "timestamp")),
              limit = Some(LimitOperator(1))
            ))
      }
    }

    "receive a complex select containing inner parenthesis" should {
      "parse it correctly" in {
        parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "SELECT name FROM people WHERE ((name like $an$ and surname = pippo) and timestamp IN (2,4)) and code is not null  ORDER BY name DESC LIMIT 5"
        ) should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 = TupledLogicalExpression(
                expression1 = TupledLogicalExpression(expression1 = LikeExpression("name", "$an$"),
                                                      operator = AndOperator,
                                                      expression2 = EqualityExpression("surname", "pippo")),
                operator = AndOperator,
                expression2 = RangeExpression("timestamp", 2, 4)
              ),
              expression2 = UnaryLogicalExpression(NullableExpression("code"), NotOperator),
              operator = AndOperator
            ))),
            order = Some(DescOrderOperator(dimension = "name")),
            limit = Some(LimitOperator(5))
          )))
      }
    }
  }
}