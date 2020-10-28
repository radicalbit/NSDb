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

package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{SumAggregation => SqlSumAggregation, _}
import io.radicalbit.nsdb.model.{Schema, TimeContext}
import io.radicalbit.nsdb.statement.FieldsParser.SimpleField
import io.radicalbit.nsdb.statement.StatementParser._
import io.radicalbit.nsdb.test.NSDbSpec
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search._

class StatementParserAggregationsSpec extends NSDbSpec {

  implicit val timeContext: TimeContext = TimeContext()

  private val schema = Schema(
    "people",
    Bit(0,
        1.1,
        dimensions = Map("name" -> "name", "surname" -> "surname", "creationDate" -> 0L),
        tags = Map("amount"     -> 1.1, "city"       -> "city", "country"         -> "country", "age" -> 0))
  )

  private val taglessSchema = Schema(
    "people",
    Bit(0, 1.1, dimensions = Map("name" -> "name", "surname" -> "surname", "creationDate" -> 0L), tags = Map.empty))

  "A statement parser instance" when {

    "receive a list of fields without a group by" should {
      "parse it successfully with mixed count(*) aggregation and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(CountAggregation("value"))), Field("surname", None))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedGlobalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              4,
              List(SimpleField("surname")),
              List(CountAggregation("value"))
            ))
        )
      }

      "parse it successfully with mixed min(*) and count(*) aggregations and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(
              List(Field("*", Some(CountAggregation("value"))),
                   Field("surname", None),
                   Field("*", Some(MinAggregation("value"))))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedGlobalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              4,
              List(SimpleField("surname")),
              List(CountAggregation("value"), MinAggregation("value"))
            ))
        )
      }

      "parse it successfully with mixed average aggregation and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(AvgAggregation("value"))), Field("surname", None))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedGlobalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              4,
              List(SimpleField("surname")),
              List(AvgAggregation("value"))
            ))
        )
      }

      "parse it successfully with mixed aggregations and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(
              List(Field("*", Some(CountAggregation("value"))),
                   Field("*", Some(AvgAggregation("value"))),
                   Field("surname", None))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedGlobalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              4,
              List(SimpleField("surname")),
              List(CountAggregation("value"), AvgAggregation("value"))
            ))
        )
      }

      "fail when any unsupported aggregation is provided" in {
        StatementParser
          .parseStatement(
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(CountAggregation("value"))),
                     Field("surname", None),
                     Field("value", Some(SqlSumAggregation("value"))))),
              limit = Some(LimitOperator(4))
            ),
            schema
          ) shouldBe Left(StatementParserErrors.NO_GROUP_BY_AGGREGATION)
      }
    }

    "receive a list of fields without a group by for a tagless metric" should {
      "parse it successfully with mixed count aggregated and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(CountAggregation("value"))), Field("surname", None))),
            limit = Some(LimitOperator(4))
          ),
          taglessSchema
        ) should be(
          Right(
            ParsedGlobalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              4,
              List(SimpleField("surname")),
              List(CountAggregation("value"))
            ))
        )
      }

      "parse it successfully if average is provided" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(AvgAggregation("value"))))),
            limit = Some(LimitOperator(4))
          ),
          taglessSchema
        ) should be(
          Right(
            ParsedGlobalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              4,
              List(),
              List(AvgAggregation("value"))
            ))
        )
      }
    }

    "receive a select containing a range selection and a group by" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", Some(SqlSumAggregation("value"))))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            groupBy = Some(SimpleGroupByAggregation("age"))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              InternalStandardAggregation("age", SqlSumAggregation("value"))
            ))
        )
      }
      "parse it successfully with sum(*)" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(SqlSumAggregation("value"))))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            groupBy = Some(SimpleGroupByAggregation("country"))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              InternalStandardAggregation("country", SqlSumAggregation("value"))
            ))
        )
      }
      "parse it successfully with avg(*)" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(AvgAggregation("value"))))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            groupBy = Some(SimpleGroupByAggregation("country"))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              InternalStandardAggregation("country", AvgAggregation("value"))
            ))
        )
      }
      "parse it successfully with first(*)" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(FirstAggregation("value"))))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            groupBy = Some(SimpleGroupByAggregation("age"))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              InternalStandardAggregation("age", FirstAggregation("value"))
            ))
        )
      }
      "parse it successfully with last(*)" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(LastAggregation("value"))))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            groupBy = Some(SimpleGroupByAggregation("country"))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              InternalStandardAggregation("country", LastAggregation("value"))
            ))
        )
      }
      "parse it successfully with max(*)" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(MaxAggregation("value"))))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            groupBy = Some(SimpleGroupByAggregation("country"))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              InternalStandardAggregation("country", MaxAggregation("value"))
            ))
        )
      }
      "parse it successfully with min(*)" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(MinAggregation("value"))))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            groupBy = Some(SimpleGroupByAggregation("country"))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              InternalStandardAggregation("country", MinAggregation("value"))
            ))
        )
      }
    }

    "receive a complex select containing a range selection a desc ordering statement, a limit statement and a group by" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", Some(MaxAggregation("value"))))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            groupBy = Some(SimpleGroupByAggregation("country")),
            order = Some(DescOrderOperator(dimension = "value")),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2L, 4L),
              InternalStandardAggregation("country", MaxAggregation("value")),
              Some(new Sort(new SortField("value", SortField.Type.DOUBLE, true))),
              Some(5)
            ))
        )
      }
    }

    "receive a group by on a tag of type different from VARCHAR()" should {
      "succeed" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", Some(SqlSumAggregation("value"))))),
            condition = Some(Condition(NullableExpression(dimension = "creationDate"))),
            groupBy = Some(SimpleGroupByAggregation("amount")),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedAggregatedQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(LongPoint.newRangeQuery("creationDate", Long.MinValue, Long.MaxValue),
                     BooleanClause.Occur.MUST_NOT)
                .build(),
              InternalStandardAggregation("amount", SqlSumAggregation("value")),
              None,
              Some(5)
            ))
        )
      }
    }

    "receive a group by with aggregation function on dimension different from value" should {
      "fail" in {
        StatementParser
          .parseStatement(
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("amount", Some(SqlSumAggregation("value"))))),
              condition = Some(Condition(NullableExpression(dimension = "creationDate"))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              limit = Some(LimitOperator(5))
            ),
            schema
          ) shouldBe Left(StatementParserErrors.AGGREGATION_NOT_ALLOWED)
      }
    }

    "receive a group by with group field different than a tag" should {
      "fail" in {
        StatementParser
          .parseStatement(
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(List(Field("*", Some(SqlSumAggregation("value"))))),
              condition = Some(Condition(NullableExpression(dimension = "creationDate"))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              limit = Some(LimitOperator(5))
            ),
            schema
          ) shouldBe Left(StatementParserErrors.SIMPLE_AGGREGATION_NOT_ON_TAG)
      }
    }

    "receive a temporal group by" should {
      "parse it when count aggregation is provided" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(CountAggregation("value"))))),
            condition = None,
            groupBy = Some(TemporalGroupByAggregation(1000, 1, "s")),
            limit = None
          ),
          schema
        ) should be(
          Right(
            ParsedTemporalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              1000,
              InternalTemporalAggregation(CountAggregation("value")),
              None
            ))
        )
      }

      "parse it when count aggregation is provided with different interval" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(CountAggregation("value"))))),
            condition = None,
            groupBy = Some(TemporalGroupByAggregation(86400000, 1, "d")),
            limit = None
          ),
          schema
        ) should be(
          Right(
            ParsedTemporalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              86400000,
              InternalTemporalAggregation(CountAggregation("value")),
              None
            ))
        )
      }

      "parse it when sum aggregation is provided" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(SqlSumAggregation("value"))))),
            condition = None,
            groupBy = Some(TemporalGroupByAggregation(1000, 1, "s")),
            limit = None
          ),
          schema
        ) should be(
          Right(
            ParsedTemporalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              1000,
              InternalTemporalAggregation(SqlSumAggregation("value")),
              None
            ))
        )
      }

      "parse it when avg aggregation is provided" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(AvgAggregation("value"))))),
            condition = None,
            groupBy = Some(TemporalGroupByAggregation(1000, 1, "s")),
            limit = None
          ),
          schema
        ) should be(
          Right(
            ParsedTemporalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              1000,
              InternalTemporalAggregation(AvgAggregation("value")),
              None
            ))
        )
      }

      "parse it when min or max aggregation is provided" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(MinAggregation("value"))))),
            condition = None,
            groupBy = Some(TemporalGroupByAggregation(1000, 1, "s")),
            limit = None
          ),
          schema
        ) shouldBe
          Right(
            ParsedTemporalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              1000,
              InternalTemporalAggregation(MinAggregation("value")),
              None
            ))

        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(MaxAggregation("value"))))),
            condition = None,
            groupBy = Some(TemporalGroupByAggregation(1000, 1, "s")),
            limit = None
          ),
          schema
        ) shouldBe Right(
          ParsedTemporalAggregatedQuery(
            "registry",
            "people",
            new MatchAllDocsQuery(),
            1000,
            InternalTemporalAggregation(MaxAggregation("value")),
            None
          ))
      }
    }

    "receive a statement with a grace period" should {
      "refuse it in case of plain queries" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = AllFields(),
            gracePeriod = Some(GracePeriod("S", 10))
          ),
          schema
        ) should be(
          Left(StatementParserErrors.GRACE_PERIOD_NOT_ALLOWED)
        )
      }
      "refuse it in case of standard group by" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = AllFields(),
            groupBy = Some(SimpleGroupByAggregation("name")),
            gracePeriod = Some(GracePeriod("S", 10))
          ),
          schema
        ) should be(
          Left(StatementParserErrors.GRACE_PERIOD_NOT_ALLOWED)
        )
      }
      "parse it successfully in case of a temporal group by" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(MinAggregation("value"))))),
            condition = None,
            groupBy = Some(TemporalGroupByAggregation(1000, 1, "s")),
            gracePeriod = Some(GracePeriod("S", 10))
          ),
          schema
        ) shouldBe
          Right(
            ParsedTemporalAggregatedQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              1000,
              InternalTemporalAggregation(MinAggregation("value")),
              None,
              gracePeriod = Some(10000)
            ))
      }
    }
  }
}
