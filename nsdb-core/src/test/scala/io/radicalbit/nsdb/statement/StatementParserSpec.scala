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

import io.radicalbit.nsdb.common.protocol.{DimensionFieldType, TagFieldType, TimestampFieldType, ValueFieldType}
import io.radicalbit.nsdb.common.statement.{SumAggregation => SqlSumAggregation, _}
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.statement.StatementParser._
import org.apache.lucene.document.{DoublePoint, LongPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.scalatest.{Matchers, WordSpec}

class StatementParserSpec extends WordSpec with Matchers {

  val schema = Schema(
    "people",
    Map(
      "timestamp"    -> SchemaField("timestamp", TimestampFieldType, BIGINT()),
      "value"        -> SchemaField("value", ValueFieldType, DECIMAL()),
      "name"         -> SchemaField("name", DimensionFieldType, VARCHAR()),
      "surname"      -> SchemaField("surname", DimensionFieldType, VARCHAR()),
      "amount"       -> SchemaField("amount", TagFieldType, DECIMAL()),
      "creationDate" -> SchemaField("creationDate", DimensionFieldType, BIGINT()),
      "city"         -> SchemaField("city", TagFieldType, VARCHAR()),
      "country"      -> SchemaField("country", TagFieldType, VARCHAR()),
      "age"          -> SchemaField("age", TagFieldType, INT())
    )
  )

  "A statement parser instance" when {

    "receive a select projecting a wildcard" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
                             distinct = false,
                             fields = AllFields(),
                             limit = Some(LimitOperator(4))),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              distinct = false,
              4
            ))
        )
      }
    }

    "receive a select projecting a not existing dimension" should {
      "fail" in {
        StatementParser
          .parseStatement(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = false,
                               fields = ListFields(List(Field("address", None))),
                               limit = Some(LimitOperator(4))),
            schema
          ) shouldBe Left(StatementParserErrors.notExistingDimensions(List("address")))
      }
    }

    "receive a select projecting a wildcard with distinct" should {
      "fail" in {
        StatementParser
          .parseStatement(
            SelectSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               distinct = true,
                               fields = AllFields(),
                               limit = Some(LimitOperator(4))),
            schema
          ) shouldBe Left(StatementParserErrors.MORE_FIELDS_DISTINCT)
      }
    }

    "receive a select projecting a single dimension with distinct" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
                             distinct = true,
                             fields = ListFields(List(Field("name", None))),
                             limit = Some(LimitOperator(4))),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              distinct = true,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
    }

    "receive a select projecting a list of fields" should {
      "parse it successfully only in simple fields" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None), Field("surname", None), Field("creationDate", None))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              distinct = false,
              4,
              List("name", "surname", "creationDate").map(SimpleField(_))
            ))
        )
      }
      "fail if distinct (Not supported yet)" in {
        StatementParser
          .parseStatement(
            SelectSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              distinct = true,
              fields = ListFields(List(Field("name", None), Field("surname", None), Field("creationDate", None))),
              limit = Some(LimitOperator(4))
            ),
            schema
          ) shouldBe Left(StatementParserErrors.MORE_FIELDS_DISTINCT)
      }

      "parse it successfully with mixed aggregated and simple" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(
              List(Field("*", Some(CountAggregation)),
                   Field("surname", None),
                   Field("creationDate", Some(CountAggregation)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              distinct = false,
              4,
              List(SimpleField("*", true), SimpleField("surname"), SimpleField("creationDate", true))
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

    "receive a select containing a range selection" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              false,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
    }

    "receive a select containing a = selection" should {
      "parse it successfully on a number vs a number" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition =
              Some(Condition(EqualityExpression(dimension = "timestamp", value = AbsoluteComparisonValue(10L)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              LongPoint.newExactQuery("timestamp", 10L),
              false,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
      "parse it successfully on a string vs a string" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition =
              Some(Condition(EqualityExpression(dimension = "name", value = AbsoluteComparisonValue("TestString")))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new TermQuery(new Term("name", "TestString")),
              false,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
      "parse it successfully on a DECIMAL() vs a int" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("amount", None))),
            condition = Some(Condition(EqualityExpression(dimension = "amount", value = AbsoluteComparisonValue(0)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              DoublePoint.newExactQuery("amount", 0),
              false,
              4,
              List("amount").map(SimpleField(_))
            ))
        )
      }
      "fail on a int vs a DECIMAL()" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition =
              Some(Condition(EqualityExpression(dimension = "creationDate", value = AbsoluteComparisonValue(0.5)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) shouldBe Left(StatementParserErrors.uncompatibleOperator("equality", "BIGINT"))
      }
      "parse it successfully on a number vs a string" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(EqualityExpression(dimension = "name", value = AbsoluteComparisonValue(0)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new TermQuery(new Term("name", "0")),
              false,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
    }

    "receive a select containing a GTE selection" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(
              Condition(
                ComparisonExpression(dimension = "timestamp",
                                     comparison = GreaterOrEqualToOperator,
                                     value = AbsoluteComparisonValue(10L)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 10L, Long.MaxValue),
              false,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
      "parse it successfully on a DECIMAL() vs a int" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("amount", None))),
            condition = Some(
              Condition(
                ComparisonExpression(dimension = "amount",
                                     comparison = GreaterOrEqualToOperator,
                                     value = AbsoluteComparisonValue(10L)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              DoublePoint.newRangeQuery("amount", 10.0, Double.MaxValue),
              false,
              4,
              List("amount").map(SimpleField(_))
            ))
        )
      }
      "fail on a int vs a DECIMAL()" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition =
              Some(Condition(EqualityExpression(dimension = "creationDate", value = AbsoluteComparisonValue(0.5)))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) shouldBe Left(StatementParserErrors.uncompatibleOperator("equality", "BIGINT"))
      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
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
            ))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(LongPoint.newRangeQuery("timestamp", 2L + 1, Long.MaxValue), BooleanClause.Occur.MUST)
                .add(LongPoint.newRangeQuery("timestamp", Long.MinValue, 4L), BooleanClause.Occur.MUST)
                .build(),
              false,
              4,
              List("name").map(SimpleField(_))
            )
          ))
      }
    }

    "receive a select containing a GTE OR a LT selection" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(
              Condition(
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
                ))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery, BooleanClause.Occur.MUST)
                .add(
                  new BooleanQuery.Builder()
                    .add(LongPoint.newRangeQuery("timestamp", 2L, Long.MaxValue), BooleanClause.Occur.SHOULD)
                    .add(LongPoint.newRangeQuery("timestamp", Long.MinValue, 4L - 1), BooleanClause.Occur.SHOULD)
                    .build(),
                  BooleanClause.Occur.MUST_NOT
                )
                .build(),
              false,
              4,
              List("name").map(SimpleField(_))
            )
          )
        )
      }
    }

    "receive a select containing a GTE OR a = selection" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(
              Condition(
                NotExpression(
                  expression = TupledLogicalExpression(
                    expression1 = ComparisonExpression(dimension = "timestamp",
                                                       comparison = GreaterOrEqualToOperator,
                                                       value = AbsoluteComparisonValue(2L)),
                    operator = OrOperator,
                    expression2 = EqualityExpression(dimension = "name", value = AbsoluteComparisonValue("$john$"))
                  )
                ))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery, BooleanClause.Occur.MUST)
                .add(
                  new BooleanQuery.Builder()
                    .add(LongPoint.newRangeQuery("timestamp", 2L, Long.MaxValue), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("name", "$john$")), BooleanClause.Occur.SHOULD)
                    .build(),
                  BooleanClause.Occur.MUST_NOT
                )
                .build(),
              false,
              4,
              List("name").map(SimpleField(_))
            )
          )
        )
      }
    }

    "receive a select containing a GTE OR a like selection" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(
              Condition(
                NotExpression(
                  expression = TupledLogicalExpression(
                    expression1 = ComparisonExpression(dimension = "timestamp",
                                                       comparison = GreaterOrEqualToOperator,
                                                       value = AbsoluteComparisonValue(2L)),
                    operator = OrOperator,
                    expression2 = LikeExpression(dimension = "name", value = "$john$")
                  )
                ))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery, BooleanClause.Occur.MUST)
                .add(
                  new BooleanQuery.Builder()
                    .add(LongPoint.newRangeQuery("timestamp", 2L, Long.MaxValue), BooleanClause.Occur.SHOULD)
                    .add(new WildcardQuery(new Term("name", "*john*")), BooleanClause.Occur.SHOULD)
                    .build(),
                  BooleanClause.Occur.MUST_NOT
                )
                .build(),
              false,
              4,
              List("name").map(SimpleField(_))
            )
          )
        )
      }
    }

    "receive a select containing a ordering statement and a limit statement" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
                             distinct = false,
                             fields = AllFields(),
                             order = Some(AscOrderOperator("name")),
                             limit = Some(LimitOperator(4))),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              false,
              4,
              List.empty,
              Some(new Sort(new SortField("name", SortField.Type.STRING, false)))
            ))
        )
      }
    }

    "receive a select containing a ordering by value statement and a limit statement" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
                             distinct = false,
                             fields = AllFields(),
                             order = Some(AscOrderOperator("value")),
                             limit = Some(LimitOperator(4))),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              false,
              4,
              List.empty,
              Some(new Sort(new SortField("value", SortField.Type.DOUBLE, false)))
            ))
        )
      }
    }

    "receive a complex select containing a range selection a desc ordering statement and a limit statement" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(
              Condition(
                RangeExpression(dimension = "timestamp",
                                value1 = AbsoluteComparisonValue(2L),
                                value2 = AbsoluteComparisonValue(4L)))),
            order = Some(DescOrderOperator(dimension = "creationDate")),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2L, 4L),
              false,
              5,
              List("name").map(SimpleField(_)),
              Some(new Sort(new SortField("creationDate", SortField.Type.LONG, true)))
            ))
        )
      }
    }

    "receive a statement without limit" should {
      "fail" in {
        StatementParser.parseStatement(SelectSQLStatement(db = "db",
                                                          namespace = "registry",
                                                          metric = "people",
                                                          distinct = false,
                                                          fields = AllFields()),
                                       schema) should be(
          Right(
            ParsedSimpleQuery("registry", "people", new MatchAllDocsQuery(), false, Int.MaxValue)
          )
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
              new InternalSumSimpleAggregation("age", "value")
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
              new InternalSumSimpleAggregation("country", "value")
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
              new InternalFirstSimpleAggregation("age", "value")
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
              new InternalLastSimpleAggregation("country", "value")
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
              new InternalMaxSimpleAggregation("country", "value"),
              Some(new Sort(new SortField("value", SortField.Type.DOUBLE, true))),
              Some(5)
            ))
        )
      }
    }

    "receive a select containing a not nullable expression on string" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", None))),
            condition = Some(Condition(NotExpression(NullableExpression(dimension = "name")))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(
                  new BooleanQuery.Builder()
                    .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                    .add(new WildcardQuery(new Term("name", "*")), BooleanClause.Occur.MUST_NOT)
                    .build(),
                  BooleanClause.Occur.MUST_NOT
                )
                .build(),
              false,
              5,
              List("value").map(SimpleField(_))
            ))
        )
      }
    }
    "receive a select containing a nullable expression on string" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", None))),
            condition = Some(Condition(NullableExpression(dimension = "name"))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(new WildcardQuery(new Term("name", "*")), BooleanClause.Occur.MUST_NOT)
                .build(),
              false,
              5,
              List("value").map(SimpleField(_))
            ))
        )
      }
    }
    "receive a select containing a not nullable expression on DECIMAL()" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", None))),
            condition = Some(Condition(NotExpression(NullableExpression(dimension = "value")))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(
                  new BooleanQuery.Builder()
                    .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                    .add(DoublePoint.newRangeQuery("value", Double.MinValue, Double.MaxValue),
                         BooleanClause.Occur.MUST_NOT)
                    .build(),
                  BooleanClause.Occur.MUST_NOT
                )
                .build(),
              false,
              5,
              List("value").map(SimpleField(_))
            ))
        )
      }
    }
    "receive a select containing a nullable expression on DECIMAL()" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", None))),
            condition = Some(Condition(NullableExpression(dimension = "value"))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(DoublePoint.newRangeQuery("value", Double.MinValue, Double.MaxValue), BooleanClause.Occur.MUST_NOT)
                .build(),
              false,
              5,
              List("value").map(SimpleField(_))
            ))
        )
      }
    }
    "receive a select containing a not nullable expression on long" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", None))),
            condition = Some(Condition(NotExpression(NullableExpression(dimension = "creationDate")))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(
                  new BooleanQuery.Builder()
                    .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                    .add(LongPoint.newRangeQuery("creationDate", Long.MinValue, Long.MaxValue),
                         BooleanClause.Occur.MUST_NOT)
                    .build(),
                  BooleanClause.Occur.MUST_NOT
                )
                .build(),
              false,
              5,
              List("value").map(SimpleField(_))
            ))
        )
      }
    }
    "receive a select containing a nullable expression on long" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", None))),
            condition = Some(Condition(NullableExpression(dimension = "creationDate"))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(LongPoint.newRangeQuery("creationDate", Long.MinValue, Long.MaxValue),
                     BooleanClause.Occur.MUST_NOT)
                .build(),
              false,
              5,
              List("value").map(SimpleField(_))
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
              new InternalSumSimpleAggregation("amount", "value"),
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
          ) shouldBe Left(StatementParserErrors.notExistingDimension("name"))
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
              InternalCountTemporalAggregation,
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
              InternalCountTemporalAggregation,
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
              InternalSumTemporalAggregation,
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
              InternalMinTemporalAggregation,
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
            InternalMaxTemporalAggregation,
            None
          ))
      }
    }
  }
}
