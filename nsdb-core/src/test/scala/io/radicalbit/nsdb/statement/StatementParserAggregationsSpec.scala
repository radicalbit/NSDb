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
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search._
import org.scalatest.{Matchers, WordSpec}

class StatementParserAggregationsSpec extends WordSpec with Matchers {

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
      "parse it successfully with mixed count aggregated and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(CountAggregation)), Field("surname", None))),
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
              List(CountAggregation)
            ))
        )
      }

      "parse it successfully with mixed average aggregated and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(AvgAggregation)), Field("surname", None))),
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
              List(AvgAggregation)
            ))
        )
      }

      "parse it successfully with mixed aggregated and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(
              List(Field("*", Some(CountAggregation)), Field("*", Some(AvgAggregation)), Field("surname", None))),
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
              List(CountAggregation, AvgAggregation)
            ))
        )
      }

      "fail when other aggregation than count is provided" in {
        StatementParser
          .parseStatement(
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = false,
              fields = ListFields(
                List(Field("*", Some(CountAggregation)),
                     Field("surname", None),
                     Field("creationDate", Some(SqlSumAggregation)))),
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
            fields = ListFields(List(Field("*", Some(CountAggregation)), Field("surname", None))),
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
              List(CountAggregation)
            ))
        )
      }

      "fail if average is provided" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(AvgAggregation)))),
            limit = Some(LimitOperator(4))
          ),
          taglessSchema
        ) should be(
          Left(StatementParserErrors.TAGLESS_AGGREGATIONS)
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
            fields = ListFields(List(Field("value", Some(SqlSumAggregation)))),
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
              InternalStandardAggregation("age", SqlSumAggregation)
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
            fields = ListFields(List(Field("*", Some(SqlSumAggregation)))),
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
              InternalStandardAggregation("country", SqlSumAggregation)
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
            fields = ListFields(List(Field("*", Some(AvgAggregation)))),
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
              InternalStandardAggregation("country", AvgAggregation)
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
            fields = ListFields(List(Field("*", Some(FirstAggregation)))),
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
              InternalStandardAggregation("age", FirstAggregation)
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
            fields = ListFields(List(Field("*", Some(LastAggregation)))),
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
              InternalStandardAggregation("country", LastAggregation)
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
            fields = ListFields(List(Field("*", Some(MaxAggregation)))),
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
              InternalStandardAggregation("country", MaxAggregation)
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
            fields = ListFields(List(Field("*", Some(MinAggregation)))),
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
              InternalStandardAggregation("country", MinAggregation)
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
            fields = ListFields(List(Field("value", Some(MaxAggregation)))),
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
              InternalStandardAggregation("country", MaxAggregation),
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
            fields = ListFields(List(Field("value", Some(SqlSumAggregation)))),
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
              InternalStandardAggregation("amount", SqlSumAggregation),
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
              fields = ListFields(List(Field("amount", Some(SqlSumAggregation)))),
              condition = Some(Condition(NullableExpression(dimension = "creationDate"))),
              groupBy = Some(SimpleGroupByAggregation("name")),
              limit = Some(LimitOperator(5))
            ),
            schema
          ) shouldBe Left(StatementParserErrors.AGGREGATION_NOT_ON_VALUE)
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
              fields = ListFields(List(Field("*", Some(SqlSumAggregation)))),
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
            fields = ListFields(List(Field("*", Some(CountAggregation)))),
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
              InternalTemporalAggregation(CountAggregation),
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
            fields = ListFields(List(Field("*", Some(CountAggregation)))),
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
              InternalTemporalAggregation(CountAggregation),
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
            fields = ListFields(List(Field("*", Some(SqlSumAggregation)))),
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
              InternalTemporalAggregation(SqlSumAggregation),
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
            fields = ListFields(List(Field("*", Some(AvgAggregation)))),
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
              InternalTemporalAggregation(AvgAggregation),
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
            fields = ListFields(List(Field("*", Some(MinAggregation)))),
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
              InternalTemporalAggregation(MinAggregation),
              None
            ))

        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(MaxAggregation)))),
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
            InternalTemporalAggregation(MaxAggregation),
            None
          ))
      }
    }
  }
}
