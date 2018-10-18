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

class SelectSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A parser instance" when {

    "receive a select projecting a wildcard" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT * FROM people") should be(
          Success(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = AllFields)))
      }
    }

    "receive a select projecting a wildcard with distinct" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT DISTINCT * FROM people") should be(
          Success(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = true,
                               fields = AllFields)))
      }
    }

    "receive a select projecting a single field" should {
      "parse it successfully with a simple field" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people") should be(
          Success(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = ListFields(List(Field("name", None)))))
        )
      }
      "parse it successfully with a simple field with distinct" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT DISTINCT name FROM people") should be(
          Success(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = true,
                               fields = ListFields(List(Field("name", None)))))
        )
      }
      "parse it successfully with an aggregated field" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT count(value) FROM people") should be(
          Success(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = ListFields(List(Field("value", Some(CountAggregation))))))
        )
      }
      "parse it successfully with an aggregated *" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT count(*) FROM people") should be(
          Success(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = ListFields(List(Field("*", Some(CountAggregation))))))
        )
      }
    }

    "receive a select projecting a list of fields" should {
      "parse it successfully only with simple fields" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name,surname,creationDate FROM people") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None), Field("surname", None), Field("creationDate", None)))
          )))
      }

      "parse it successfully only with simple fields and distinct" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT DISTINCT name,surname,creationDate FROM people") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = true,
            fields = ListFields(List(Field("name", None), Field("surname", None), Field("creationDate", None)))
          )))
      }

      "parse it successfully with mixed aggregated and simple" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT count(*),surname,sum(creationDate) FROM people") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(CountAggregation)),
                                     Field("surname", None),
                                     Field("creationDate", Some(SumAggregation))))
          )))
      }
    }

    "receive a select containing a range selection" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp IN (2,4)") should be(
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
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp IN (2, 3.5)") should be(
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
                                     input = "SELECT name FROM people WHERE timestamp IN (now - 2 s, now + 4 s)")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a = selection" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp = 10") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(EqualityExpression(dimension = "timestamp", value = 10L)))
          )))
      }

      "parse it successfully using decimals" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp = 10.5") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(EqualityExpression(dimension = "timestamp", value = 10.5)))
          )))
      }

      "parse it successfully using string" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp = word_word") should be(
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
                       input = "SELECT name FROM people WHERE timestamp = now - 10s")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a like selection" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE name like $ame$") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(LikeExpression(dimension = "name", value = "$ame$")))
          )))
      }

      "parse it successfully with predicate containing special characters" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE name like $a_m-e$") should be(
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
        parser.parse(db = "db", namespace = "registry", input = "SELECT name FROM people WHERE timestamp >= 10") should be(
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
                       input = "SELECT name FROM people WHERE timestamp >= now - 10s")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a GT AND a = selection" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE timestamp > 2 AND timestamp = 4") should be(
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

      "parse it successfully using decimal values" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE timestamp > 2.4 AND timestamp = 4") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 =
                ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2.4),
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
            "SELECT name FROM people WHERE timestamp < now + 30d and timestamp > now - 2h AND timestamp = now + 4m")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE timestamp > 2 AND timestamp <= 4") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = AndOperator,
              expression2 =
                ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
            )))
          )))
      }

      "parse it successfully using decimal values" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE timestamp > 2.5 AND timestamp <= 4.01") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 =
                ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2.5),
              operator = AndOperator,
              expression2 =
                ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4.01)
            )))
          )))
      }

      "parse it successfully using relative time" in {
        val statement = parser.parse(db = "db",
                                     namespace = "registry",
                                     input =
                                       "SELECT name FROM people WHERE timestamp > now - 2h AND timestamp <= now + 4m")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a GTE OR a LT selection" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE NOT timestamp >= 2 OR timestamp < 4") should be(
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

      "parse it successfully using relative time" in {
        val statement =
          parser.parse(db = "db",
                       namespace = "registry",
                       input = "SELECT name FROM people WHERE NOT timestamp >= now + 2m OR timestamp < now - 4h")
        statement.isSuccess shouldBe true
      }
    }

    "receive a select containing a ordering statement" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT * FROM people ORDER BY name") should be(
          Success(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = AllFields,
                               order = Some(AscOrderOperator("name")))))
      }
    }

    "receive a select containing a limit statement" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT * FROM people LIMIT 10") should be(
          Success(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = AllFields,
                               limit = Some(LimitOperator(10)))))
      }
    }

    "receive a complex select containing a range selection a desc ordering statement and a limit statement" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name FROM people WHERE timestamp IN (2,4) ORDER BY name DESC LIMIT 5") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2, value2 = 4))),
            order = Some(DescOrderOperator(dimension = "name")),
            limit = Some(LimitOperator(5))
          )))
      }
      "parse it successfully ignoring case" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "sElect name FrOm people where timestamp in (2,4) Order bY name dEsc limit 5") should be(
          Success(SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2, value2 = 4))),
            order = Some(DescOrderOperator(dimension = "name")),
            limit = Some(LimitOperator(5))
          )))
      }
    }

    "receive a complex select containing 3 conditions a desc ordering statement and a limit statement" should {
      "parse it successfully" in {
        parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "SELECT name FROM people WHERE name like $an$ and surname = pippo and timestamp IN (2,4)  ORDER BY name DESC LIMIT 5"
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
    }

    "receive a complex select containing a equality selection a desc ordering statement and a limit statement" in {
      parser.parse(
        db = "db",
        namespace = "registry",
        input = "select * from AreaOccupancy where name=MeetingArea order by timestamp desc limit 1") shouldBe
        Success(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "AreaOccupancy",
            distinct = false,
            fields = AllFields,
            condition = Some(Condition(EqualityExpression("name", "MeetingArea"))),
            order = Some(DescOrderOperator(dimension = "timestamp")),
            limit = Some(LimitOperator(1))
          ))
    }

    "receive a select containing condition of nullable" in {
      parser.parse(
        db = "db",
        namespace = "registry",
        input = "select * from AreaOccupancy where name=MeetingArea and name is null order by timestamp desc limit 1") shouldBe
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
                                                NullableExpression("name")))),
            order = Some(DescOrderOperator(dimension = "timestamp")),
            limit = Some(LimitOperator(1))
          ))
    }

    "receive a select containing a condition of not nullable" in {
      parser.parse(
        db = "db",
        namespace = "registry",
        input =
          "select * from AreaOccupancy where name=MeetingArea and name is not null order by timestamp desc limit 1") shouldBe
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
          "select * from AreaOccupancy where name=MeetingArea and name is not null or floor is not null order by timestamp desc limit 1"
      ) shouldBe
        Success(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "AreaOccupancy",
            distinct = false,
            fields = AllFields,
            condition = Some(Condition(TupledLogicalExpression(
              EqualityExpression("name", "MeetingArea"),
              AndOperator,
              TupledLogicalExpression(
                UnaryLogicalExpression(NullableExpression("name"), NotOperator),
                OrOperator,
                UnaryLogicalExpression(NullableExpression("floor"), NotOperator)
              )
            ))),
            order = Some(DescOrderOperator(dimension = "timestamp")),
            limit = Some(LimitOperator(1))
          ))
    }

    "receive a select with where condition on string dimension with spaces" in {
      parser.parse(db = "db",
                   namespace = "registry",
                   input = "select name from people where name = 'string spaced' limit 5") should be(
        Success(SelectSQLStatement(
          db = "db",
          namespace = "registry",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = Some(Condition(EqualityExpression(dimension = "name", value = "string spaced"))),
          limit = Some(LimitOperator(5))
        )))
    }

    "receive a select with where condition on string dimension with one char" in {
      parser.parse(db = "db", namespace = "registry", input = "select name from people where name = 'a' limit 5") should be(
        Success(SelectSQLStatement(
          db = "db",
          namespace = "registry",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = Some(Condition(EqualityExpression(dimension = "name", value = "a"))),
          limit = Some(LimitOperator(5))
        )))
    }

    "receive random string sequences" should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "fkjdskjfdlsf") shouldBe 'failure
      }
    }

    "receive wrong fields" should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name surname FROM people") shouldBe 'failure
        parser.parse(db = "db", namespace = "registry", input = "SELECT name,surname age FROM people") shouldBe 'failure
      }
    }

    "receive query with distinct in wrong order " should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name, distinct surname FROM people") shouldBe 'failure
      }
    }

    "receive a wrong metric without where clause" should {
      "fail" in {
        val f = parser.parse(db = "db", namespace = "registry", input = "SELECT name,surname FROM people cats dogs")
        f shouldBe 'failure
      }
    }

    "receive a wrong metric with where clause" should {
      "fail" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name,surname FROM people cats dogs WHERE timestamp > 10") shouldBe 'failure
      }
    }
  }
}
