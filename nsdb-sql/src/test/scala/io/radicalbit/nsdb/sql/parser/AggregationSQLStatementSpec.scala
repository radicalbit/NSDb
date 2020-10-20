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

class AggregationSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A SQL parser instance" when {

    "receive a select with one aggregation and without a group by" should {
      "parse it successfully when avg(value) is provided" in {
        val query = "SELECT avg(value) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(AvgAggregation("value"))))),
              groupBy = None
            )
          ))
      }

      "parse it successfully when avg(*) aggregation is provided" in {
        val query = "SELECT avg(*) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("*", Some(AvgAggregation("value"))))),
              groupBy = None
            )
          ))
      }

      "parse it successfully when avg(*) aggregation with a where condition is provided" in {
        val query = "SELECT avg(*) FROM people WHERE timestamp IN (2,4)"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("*", Some(AvgAggregation("value"))))),
              condition = Some(
                Condition(RangeExpression(dimension = "timestamp",
                                          value1 = AbsoluteComparisonValue(2L),
                                          value2 = AbsoluteComparisonValue(4L)))),
              groupBy = None
            )
          ))
      }

      "parse it successfully when min(value) is provided" in {
        val query = "SELECT min(value) FROM people"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(MinAggregation("value"))))),
              groupBy = None
            )
          ))
      }

      "parse it successfully when mixed min(*), count(*) aggregations and a where condition are provided" in {
        val query = "SELECT count(*), min(value) FROM people WHERE timestamp IN (2,4)"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(CountAggregation("value"))), Field("value", Some(MinAggregation("value"))))),
              condition = Some(
                Condition(RangeExpression(dimension = "timestamp",
                                          value1 = AbsoluteComparisonValue(2L),
                                          value2 = AbsoluteComparisonValue(4L)))),
              groupBy = None
            )
          ))
      }
    }

    "receive a select with a group by and one aggregation" should {
      "parse it successfully" in {
        val query = "SELECT sum(value) FROM people group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
      "parse it successfully if sum(*) is provided" in {
        val query = "SELECT sum(*) FROM people group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("*", Some(SumAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
      "parse it successfully if count(*) is provided" in {
        val query = "SELECT count(*) FROM people group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("*", Some(CountAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
      "parse it successfully if min(*) is provided" in {
        val query = "SELECT min(*) FROM people group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("*", Some(MinAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
      "parse it successfully if avg(*) is provided" in {
        val query = "SELECT avg(*) FROM people group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("*", Some(AvgAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
    }

    "receive a select with a group by and one distinct aggregation" should {
      "parse it successfully in case of count" in {
        val query = "SELECT count( distinct value) FROM people group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountDistinctAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
      "parse it successfully in case of count * " in {
        val query = "SELECT count( distinct *) FROM people group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("*", Some(CountDistinctAggregation("value"))))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
      "fail in case of other aggregations" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT sum( distinct *) FROM people group by name") shouldBe a[
          SqlStatementParserFailure]
        parser.parse(db = "db", namespace = "registry", input = "SELECT min( distinct *) FROM people group by name") shouldBe a[
          SqlStatementParserFailure]
        parser.parse(db = "db", namespace = "registry", input = "SELECT max( distinct *) FROM people group by name") shouldBe a[
          SqlStatementParserFailure]
        parser.parse(db = "db", namespace = "registry", input = "SELECT avg( distinct *) FROM people group by name") shouldBe a[
          SqlStatementParserFailure]
      }
    }

    "receive a select containing a range selection and a group by" should {
      "parse it successfully" in {
        val query = "SELECT count(value) FROM people WHERE timestamp IN (2,4) group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              condition = Some(
                Condition(RangeExpression(dimension = "timestamp",
                                          value1 = AbsoluteComparisonValue(2L),
                                          value2 = AbsoluteComparisonValue(4L)))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
    }

    "receive a select containing a GTE selection and a group by" should {
      "parse it successfully" in {
        val query = "SELECT min(value) FROM people WHERE timestamp >= 10 group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(MinAggregation("value"))))),
              condition = Some(
                Condition(ComparisonExpression(dimension = "timestamp",
                                               comparison = GreaterOrEqualToOperator,
                                               value = AbsoluteComparisonValue(10L)))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
    }

    "receive a select containing a GT AND a LTE selection and a group by" should {
      "parse it successfully" in {
        val query = "SELECT max(value) FROM people WHERE timestamp > 2 AND timestamp <= 4 group by name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(MaxAggregation("value"))))),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(2L)),
                operator = AndOperator,
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessOrEqualToOperator,
                                                   value = AbsoluteComparisonValue(4L))
              ))),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
    }

    "receive a select containing a ordering statement" should {
      "parse it successfully" in {
        val query = "SELECT count(value) FROM people group by name ORDER BY name"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              order = Some(AscOrderOperator("name")),
              groupBy = Some(SimpleGroupByAggregation("name"))
            )
          ))
      }
    }

    "receive a select containing a ordering statement and a limit clause" should {
      "parse it successfully" in {
        val query = "SELECT count(value) FROM people group by name ORDER BY name limit 1"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              order = Some(AscOrderOperator("name")),
              groupBy = Some(SimpleGroupByAggregation("name")),
              limit = Some(LimitOperator(1))
            )
          ))
      }
    }

    "receive a select containing uuids" should {
      "parse it successfully" in {
        val query =
          "select count(value) from people where name = b483a480-832b-473e-a999-5d1a5950858d and surname = b483a480-832b group by surname"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 =
                  EqualityExpression(dimension = "name",
                                     value = AbsoluteComparisonValue("b483a480-832b-473e-a999-5d1a5950858d")),
                expression2 =
                  EqualityExpression(dimension = "surname", value = AbsoluteComparisonValue("b483a480-832b")),
                operator = AndOperator
              ))),
              groupBy = Some(SimpleGroupByAggregation("surname"))
            )
          ))

        parser.parse(
          db = "db",
          namespace = "registry",
          input =
            "select count(value) from people where na-me = b483a480-832b-473e-a999-5d1a5950858d and surname = b483a480-832b group by surname"
        ) shouldBe a[SqlStatementParserFailure]
      }
    }

    "receive a select containing uuids and more than 2 where" should {
      "parse it successfully" in {
        val query =
          "select count(value) from people where prediction = 1.0 and adoptedModel = b483a480-832b-473e-a999-5d1a5950858d and id = c1234-56789 group by id"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              condition = Some(
                Condition(
                  TupledLogicalExpression(
                    EqualityExpression(dimension = "prediction", value = AbsoluteComparisonValue(1.0)),
                    AndOperator,
                    TupledLogicalExpression(
                      EqualityExpression(dimension = "adoptedModel",
                                         value = AbsoluteComparisonValue("b483a480-832b-473e-a999-5d1a5950858d")),
                      AndOperator,
                      EqualityExpression(dimension = "id", AbsoluteComparisonValue("c1234-56789"))
                    )
                  )
                )),
              groupBy = Some(SimpleGroupByAggregation("id"))
            )
          ))
      }
    }

    "receive wrong fields" should {
      "fail" in {
        parser.parse(db = "db", namespace = "registry", input = "SELECT count(name,surname) FROM people") shouldBe a[
          SqlStatementParserFailure]
      }
    }

    "receive a select with a temporal group by with count aggregation in seconds" should {
      "parse it successfully" in {
        val query = "SELECT count(value) FROM people group by interval 3 s"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              groupBy = Some(TemporalGroupByAggregation(3000, 3, "S"))
            )
          ))
      }
    }

    "receive a select with a temporal group by without measure in minutes" should {
      "parse it successfully" in {
        val query = "SELECT count(value) FROM people group by interval min"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
              groupBy = Some(TemporalGroupByAggregation(60000, 1, "MIN"))
            )
          ))
      }
    }

    "receive a select with a temporal group by with count aggregation with a limit" in {
      val query = "select count(value) from people group by interval 1d limit 1"
      parser.parse(db = "db", namespace = "registry", input = query) should be(
        SqlStatementParserSuccess(
          query,
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
            groupBy = Some(TemporalGroupByAggregation(86400000, 1, "D")),
            limit = Some(LimitOperator(1))
          )
        ))
    }

    "receive a select with a temporal group by, filtered by time with measure" should {
      "parse it successfully if the interval contains a space" in {
        val query = "SELECT count(*) FROM people WHERE timestamp > 1 and timestamp < 100 group by interval 2 d"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(1L)),
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessThanOperator,
                                                   value = AbsoluteComparisonValue(100L)),
                operator = AndOperator
              ))),
              fields = ListFields(List(Field("*", Some(CountAggregation("value"))))),
              groupBy = Some(TemporalGroupByAggregation(2 * 24 * 3600 * 1000, 2, "D"))
            )
          ))
      }

      "parse it successfully if the interval does not contain any space" in {
        val query = "SELECT count(*) FROM people WHERE timestamp > 1 and timestamp < 100 group by interval 2d"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(1L)),
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessThanOperator,
                                                   value = AbsoluteComparisonValue(100L)),
                operator = AndOperator
              ))),
              fields = ListFields(List(Field("*", Some(CountAggregation("value"))))),
              groupBy = Some(TemporalGroupByAggregation(2 * 24 * 3600 * 1000, 2, "D"))
            )
          ))
      }

      "parse it successfully if an aggregation different from count is provided " in {
        val query = "SELECT sum(*) FROM people WHERE timestamp > 1 and timestamp < 100 group by interval 2d"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(1L)),
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessThanOperator,
                                                   value = AbsoluteComparisonValue(100L)),
                operator = AndOperator
              ))),
              fields = ListFields(List(Field("*", Some(SumAggregation("value"))))),
              groupBy = Some(TemporalGroupByAggregation(2 * 24 * 3600 * 1000, 2, "D"))
            )
          ))
      }

      "parse it successfully if an avg aggregation is provided" in {
        val query = "SELECT avg(*) FROM people WHERE timestamp > 1 and timestamp < 100 group by interval 4d"
        parser.parse(db = "db", namespace = "registry", input = query) should be(
          SqlStatementParserSuccess(
            query,
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              condition = Some(Condition(TupledLogicalExpression(
                expression1 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = GreaterThanOperator,
                                                   value = AbsoluteComparisonValue(1L)),
                expression2 = ComparisonExpression(dimension = "timestamp",
                                                   comparison = LessThanOperator,
                                                   value = AbsoluteComparisonValue(100L)),
                operator = AndOperator
              ))),
              fields = ListFields(List(Field("*", Some(AvgAggregation("value"))))),
              groupBy = Some(TemporalGroupByAggregation(4 * 24 * 3600 * 1000, 4, "D"))
            )
          ))
      }
    }
  }
}
