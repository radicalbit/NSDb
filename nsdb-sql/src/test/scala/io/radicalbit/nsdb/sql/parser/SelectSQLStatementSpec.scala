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

class SelectSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A SQL parser instance" when {

    "receive a select projecting a wildcard" should {
      "parse it successfully" in {
        val query = "SELECT * FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    SelectSQLStatement(db = "db",
                                                       namespace = "registry",
                                                       metric = "people",
                                                       distinct = false,
                                                       fields = AllFields())))
      }
    }

    "receive a select projecting a wildcard with distinct" should {
      "parse it successfully" in {
        val query = "SELECT DISTINCT * FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    SelectSQLStatement(db = "db",
                                                       namespace = "registry",
                                                       metric = "people",
                                                       distinct = true,
                                                       fields = AllFields())))
      }
    }

    "receive a select projecting a single field" should {
      "parse it successfully with a simple field" in {
        val query = "SELECT name FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    SelectSQLStatement(db = "db",
                                                       namespace = "registry",
                                                       metric = "people",
                                                       distinct = false,
                                                       fields = ListFields(List(Field("name", None)))))
        )
      }
      "parse it successfully with a simple field with distinct" in {
        val query = "SELECT DISTINCT name FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    SelectSQLStatement(db = "db",
                                                       namespace = "registry",
                                                       metric = "people",
                                                       distinct = true,
                                                       fields = ListFields(List(Field("name", None)))))
        )
      }
      "parse it successfully with a count aggregated field" in {
        val query = "SELECT count(value) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = ListFields(List(Field("value", Some(CountAggregation))))))
        )
      }
      "parse it successfully with a sum aggregated field" in {
        val query = "SELECT sum(value) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    SelectSQLStatement(db = "db",
                                                       namespace = "registry",
                                                       metric = "people",
                                                       distinct = false,
                                                       fields = ListFields(List(Field("value", Some(SumAggregation))))))
        )
      }
      "parse it successfully with a first aggregated field" in {
        val query = "SELECT first(value) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = ListFields(List(Field("value", Some(FirstAggregation))))))
        )
      }
      "parse it successfully with a last aggregated field" in {
        val query = "SELECT last(value) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = ListFields(List(Field("value", Some(LastAggregation))))))
        )
      }
      "parse it successfully with an aggregated *" in {
        val query = "SELECT count(*) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
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
        val query = "SELECT name,surname,creationDate FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None), Field("surname", None), Field("creationDate", None)))
            )
          ))
      }

      "parse it successfully only with simple fields and distinct" in {
        val query = "SELECT DISTINCT name,surname,creationDate FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = true,
              fields = ListFields(List(Field("name", None), Field("surname", None), Field("creationDate", None)))
            )
          ))
      }

      "parse it successfully with mixed aggregated and simple" in {
        val query = "SELECT count(*),surname,sum(creationDate) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(CountAggregation)),
                     Field("surname", None),
                     Field("creationDate", Some(SumAggregation))))
            )
          ))
      }
    }

    "receive a select containing a range selection" should {
      "parse it successfully" in {
        val query = "SELECT name FROM people WHERE timestamp IN (2,4)"
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
                Condition(RangeExpression(dimension = "timestamp",
                                          value1 = AbsoluteComparisonValue(2L),
                                          value2 = AbsoluteComparisonValue(4L))))
            )
          ))
      }

      "parse it successfully using decimal values" in {
        val query = "SELECT name FROM people WHERE timestamp IN (2, 3.5)"
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
                Condition(RangeExpression(dimension = "timestamp",
                                          value1 = AbsoluteComparisonValue(2L),
                                          value2 = AbsoluteComparisonValue(3.5))))
            )
          ))
      }
    }

    "receive a select containing a GTE selection" should {
      "parse it successfully" in {
        val query = "SELECT name FROM people WHERE timestamp >= 10"
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
                Condition(ComparisonExpression(dimension = "timestamp",
                                               comparison = GreaterOrEqualToOperator,
                                               value = AbsoluteComparisonValue(10L))))
            )
          ))
      }
    }

    "receive a select containing a GT AND a = selection" should {
      "parse it successfully" in {
        val query = "SELECT name FROM people WHERE timestamp > 2 AND timestamp = 4"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(2L)),
                operator = AndOperator,
                expression2 = EqualityExpression(dimension = "timestamp", value = AbsoluteComparisonValue(4L))
              )))
            )
          ))
      }

      "parse it successfully using decimal values" in {
        val query = "SELECT name FROM people WHERE timestamp > 2.4 AND timestamp = 4"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(2.4)),
                operator = AndOperator,
                expression2 = EqualityExpression(dimension = "timestamp", value = AbsoluteComparisonValue(4L))
              )))
            )
          ))
      }

    }

    "receive a select containing a GT AND a LTE selection" should {
      "parse it successfully" in {
        val query = "SELECT name FROM people WHERE timestamp > 2 AND timestamp <= 4"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(2L)),
                operator = AndOperator,
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessOrEqualToOperator,
                                                   value = AbsoluteComparisonValue(4L))
              )))
            )
          ))
      }

      "parse it successfully using decimal values" in {
        val query = "SELECT name FROM people WHERE timestamp > 2.5 AND timestamp <= 4.01"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(2.5)),
                operator = AndOperator,
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessOrEqualToOperator,
                                                   value = AbsoluteComparisonValue(4.01))
              )))
            )
          ))
      }
    }

    "receive a select containing a GTE OR a LT selection" should {
      "parse it successfully" in {
        val query = "SELECT name FROM people WHERE NOT timestamp >= 2 OR timestamp < 4"
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
                Condition(NotExpression(
                  expression = TupledLogicalExpression(
                    expression1 = ComparisonExpression(dimension = "timestamp",
                                                       comparison = GreaterOrEqualToOperator,
                                                       value = AbsoluteComparisonValue(2L)),
                    operator = OrOperator,
                    expression2 = ComparisonExpression(dimension = "timestamp",
                                                       comparison = LessThanOperator,
                                                       value = AbsoluteComparisonValue(4L))
                  )
                )))
            )
          ))
      }
    }

    "receive a select containing a ordering statement" should {
      "parse it successfully" in {
        val query = "SELECT * FROM people ORDER BY name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    SelectSQLStatement(db = "db",
                                                       namespace = "registry",
                                                       metric = "people",
                                                       distinct = false,
                                                       fields = AllFields(),
                                                       order = Some(AscOrderOperator("name")))))
      }
    }

    "receive a select containing a limit statement" should {
      "parse it successfully" in {
        val query = "SELECT * FROM people LIMIT 10"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(query,
                                    SelectSQLStatement(db = "db",
                                                       namespace = "registry",
                                                       metric = "people",
                                                       distinct = false,
                                                       fields = AllFields(),
                                                       limit = Some(LimitOperator(10)))))
      }
    }

    "receive a complex select containing a range selection a desc ordering statement and a limit statement" should {
      "parse it successfully" in {
        val query = "SELECT name FROM people WHERE timestamp IN (2,4) ORDER BY name DESC LIMIT 5"
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
                Condition(RangeExpression(dimension = "timestamp",
                                          value1 = AbsoluteComparisonValue(2L),
                                          value2 = AbsoluteComparisonValue(4L)))),
              order = Some(DescOrderOperator(dimension = "name")),
              limit = Some(LimitOperator(5))
            )
          ))
      }
      "parse it successfully ignoring case" in {
        val query = "sElect name FrOm people where timestamp in (2,4) Order bY name dEsc limit 5"
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
                Condition(RangeExpression(dimension = "timestamp",
                                          value1 = AbsoluteComparisonValue(2L),
                                          value2 = AbsoluteComparisonValue(4L)))),
              order = Some(DescOrderOperator(dimension = "name")),
              limit = Some(LimitOperator(5))
            )
          ))
      }
    }

    "receive a complex select containing 3 conditions a desc ordering statement and a limit statement" should {
      "parse it successfully" in {
        val query =
          "SELECT name FROM people WHERE name like $an$ and surname = pippo and timestamp IN (2,4)  ORDER BY name DESC LIMIT 5"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(TupledLogicalExpression(
                LikeExpression("name", "$an$"),
                AndOperator,
                TupledLogicalExpression(
                  EqualityExpression(dimension = "surname", value = AbsoluteComparisonValue("pippo")),
                  AndOperator,
                  RangeExpression(dimension = "timestamp",
                                  value1 = AbsoluteComparisonValue(2L),
                                  AbsoluteComparisonValue(4L))
                )
              ))),
              order = Some(DescOrderOperator(dimension = "name")),
              limit = Some(LimitOperator(5))
            )
          ))
      }
    }

    "receive a complex select containing a equality selection a desc ordering statement and a limit statement" in {
      val query = "select * from AreaOccupancy where name=MeetingArea order by timestamp desc limit 1"
      parser.parse(db = "db", namespace = "registry", input = query) shouldBe
        SqlStatementParserSuccess(
          query,
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "AreaOccupancy",
            distinct = false,
            fields = AllFields(),
            condition = Some(Condition(EqualityExpression(dimension = "name", AbsoluteComparisonValue("MeetingArea")))),
            order = Some(DescOrderOperator(dimension = "timestamp")),
            limit = Some(LimitOperator(1))
          )
        )
    }

    "receive a select containing condition of nullable" in {
      val query = "select * from AreaOccupancy where name=MeetingArea and name is null order by timestamp desc limit 1"
      parser.parse(db = "db", namespace = "registry", input = query) shouldBe
        SqlStatementParserSuccess(
          query,
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "AreaOccupancy",
            distinct = false,
            fields = AllFields(),
            condition = Some(
              Condition(
                TupledLogicalExpression(EqualityExpression(dimension = "name", AbsoluteComparisonValue("MeetingArea")),
                                        AndOperator,
                                        NullableExpression("name")))),
            order = Some(DescOrderOperator(dimension = "timestamp")),
            limit = Some(LimitOperator(1))
          )
        )
    }

    "receive a select containing a condition of not nullable" in {
      val query =
        "select * from AreaOccupancy where name=MeetingArea and name is not null order by timestamp desc limit 1"
      parser.parse(db = "db", namespace = "registry", input = query) shouldBe
        SqlStatementParserSuccess(
          query,
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "AreaOccupancy",
            distinct = false,
            fields = AllFields(),
            condition = Some(
              Condition(
                TupledLogicalExpression(
                  EqualityExpression(dimension = "name", value = AbsoluteComparisonValue("MeetingArea")),
                  AndOperator,
                  NotExpression(NullableExpression("name"))
                ))),
            order = Some(DescOrderOperator(dimension = "timestamp")),
            limit = Some(LimitOperator(1))
          )
        )
    }

    "receive a select containing two conditions of not nullable" in {
      val query =
        "select * from AreaOccupancy where name=MeetingArea and name is not null or floor is not null order by timestamp desc limit 1"
      parser.parse(db = "db", namespace = "registry", input = query) shouldBe
        SqlStatementParserSuccess(
          query,
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "AreaOccupancy",
            distinct = false,
            fields = AllFields(),
            condition = Some(
              Condition(TupledLogicalExpression(
                EqualityExpression(dimension = "name", AbsoluteComparisonValue("MeetingArea")),
                AndOperator,
                TupledLogicalExpression(
                  NotExpression(NullableExpression("name")),
                  OrOperator,
                  NotExpression(NullableExpression("floor"))
                )
              ))),
            order = Some(DescOrderOperator(dimension = "timestamp")),
            limit = Some(LimitOperator(1))
          )
        )
    }

    "receive random string sequences" should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "fkjdskjfdlsf") shouldBe a[SqlStatementParserFailure]
      }
    }

    "receive wrong fields" should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name surname FROM people") shouldBe a[
          SqlStatementParserFailure]
        parser.parse(db = "db", namespace = "registry", input = "SELECT name,surname age FROM people") shouldBe a[
          SqlStatementParserFailure]
      }
    }

    "receive query with distinct in wrong order " should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT name, distinct surname FROM people") shouldBe a[
          SqlStatementParserFailure]
      }
    }

    "receive a wrong metric without where clause" should {
      "fail" in {
        val f = parser.parse(db = "db", namespace = "registry", input = "SELECT name,surname FROM people cats dogs")
        f shouldBe a[SqlStatementParserFailure]
      }
    }

    "receive a wrong metric with where clause" should {
      "fail" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "SELECT name,surname FROM people cats dogs WHERE timestamp > 10") shouldBe a[
          SqlStatementParserFailure]
      }
    }
  }
}
