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

package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.protocol.{DimensionFieldType, TagFieldType, TimestampFieldType, ValueFieldType}
import io.radicalbit.nsdb.common.statement.{SumAggregation => SqlSumAggregation, _}
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.statement.StatementParser._
import org.apache.lucene.document.{DoublePoint, LongPoint}
import org.apache.lucene.facet.range.LongRange
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.scalatest.TryValues._
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}

class StatementParserSpec extends WordSpec with Matchers {

  val schema = Schema(
    "people",
    Set(
      SchemaField("timestamp", TimestampFieldType, BIGINT()),
      SchemaField("value", ValueFieldType, DECIMAL()),
      SchemaField("name", DimensionFieldType, VARCHAR()),
      SchemaField("surname", DimensionFieldType, VARCHAR()),
      SchemaField("amount", DimensionFieldType, DECIMAL()),
      SchemaField("creationDate", DimensionFieldType, BIGINT()),
      SchemaField("city", TagFieldType, VARCHAR()),
      SchemaField("country", TagFieldType, VARCHAR()),
      SchemaField("age", TagFieldType, INT())
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
                             fields = AllFields,
                             limit = Some(LimitOperator(4))),
          schema
        ) should be(
          Success(
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
      "fails" in {
        StatementParser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
                             distinct = false,
                             fields = ListFields(List(Field("address", None))),
                             limit = Some(LimitOperator(4))),
          schema
        ) shouldBe 'failure
      }
    }

    "receive a select projecting a wildcard with distinct" should {
      "fail" in {
        StatementParser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
                             distinct = true,
                             fields = AllFields,
                             limit = Some(LimitOperator(4))),
          schema
        ) shouldBe 'failure
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
          Success(
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
          Success(
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
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = true,
            fields = ListFields(List(Field("name", None), Field("surname", None), Field("creationDate", None))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) shouldBe 'failure
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
          Success(
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
        StatementParser.parseStatement(
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
        ) shouldBe 'failure
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
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
            condition = Some(Condition(EqualityExpression(dimension = "timestamp", value = 10L))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
            condition = Some(Condition(EqualityExpression(dimension = "name", value = "TestString"))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
      "parse it successfully on a decimal vs a int" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("amount", None))),
            condition = Some(Condition(EqualityExpression(dimension = "amount", value = 0))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
      "fail on a int vs a decimal" in {
        val result = StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(EqualityExpression(dimension = "creationDate", value = 0.5))),
            limit = Some(LimitOperator(4))
          ),
          schema
        )
        result.failure.exception shouldBe a[InvalidStatementException]
      }
      "parse it successfully on a number vs a string" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(EqualityExpression(dimension = "name", value = 0))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
            condition = Some(Condition(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
      "parse it successfully on a decimal vs a int" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("amount", None))),
            condition = Some(Condition(
              ComparisonExpression(dimension = "amount", comparison = GreaterOrEqualToOperator, value = 10L))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
      "fail on a int vs a decimal" in {
        val result = StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            condition = Some(Condition(EqualityExpression(dimension = "creationDate", value = 0.5))),
            limit = Some(LimitOperator(4))
          ),
          schema
        )
        result.failure.exception shouldBe a[InvalidStatementException]
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
              expression1 = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = AndOperator,
              expression2 =
                ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
            ))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
            condition = Some(Condition(UnaryLogicalExpression(
              expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
              ),
              operator = NotOperator
            ))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
            condition = Some(Condition(UnaryLogicalExpression(
              expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = EqualityExpression(dimension = "name", value = "$john$")
              ),
              operator = NotOperator
            ))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
            condition = Some(Condition(UnaryLogicalExpression(
              expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = LikeExpression(dimension = "name", value = "$john$")
              ),
              operator = NotOperator
            ))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Success(
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
                             fields = AllFields,
                             order = Some(AscOrderOperator("name")),
                             limit = Some(LimitOperator(4))),
          schema
        ) should be(
          Success(
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
                             fields = AllFields,
                             order = Some(AscOrderOperator("value")),
                             limit = Some(LimitOperator(4))),
          schema
        ) should be(
          Success(
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
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
            order = Some(DescOrderOperator(dimension = "creationDate")),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Success(
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
                                                          fields = AllFields),
                                       schema) should be(
          Success(
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
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
            groupBy = Some("name")
          ),
          schema
        ) should be(
          Success(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              new InternalSumAggregation("name", "value")
            ))
        )
      }
      "parse it successfully with count(*)" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("*", Some(SqlSumAggregation)))),
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
            groupBy = Some("name")
          ),
          schema
        ) should be(
          Success(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, 4),
              new InternalSumAggregation("name", "value")
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
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
            groupBy = Some("name"),
            order = Some(DescOrderOperator(dimension = "value")),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Success(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2L, 4L),
              new InternalMaxAggregation("name", "value"),
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
            condition = Some(Condition(UnaryLogicalExpression(NullableExpression(dimension = "name"), NotOperator))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Success(
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
          Success(
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
    "receive a select containing a not nullable expression on decimal" should {
      "parse it successfully" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", None))),
            condition = Some(Condition(UnaryLogicalExpression(NullableExpression(dimension = "value"), NotOperator))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Success(
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
    "receive a select containing a nullable expression on decimal" should {
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
          Success(
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
            condition =
              Some(Condition(UnaryLogicalExpression(NullableExpression(dimension = "creationDate"), NotOperator))),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Success(
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
          Success(
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

    "receive a group by on a dimension of type different from varchar" should {
      "succeed" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("value", Some(SqlSumAggregation)))),
            condition = Some(Condition(NullableExpression(dimension = "creationDate"))),
            groupBy = Some("amount"),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Success(
            ParsedAggregatedQuery(
              "registry",
              "people",
              new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                .add(LongPoint.newRangeQuery("creationDate", Long.MinValue, Long.MaxValue),
                     BooleanClause.Occur.MUST_NOT)
                .build(),
              new InternalSumAggregation("amount", "value"),
              None,
              Some(5)
            ))
        )
      }
    }

    "receive a group by with aggregation function on dimension different from value" should {
      "fail" in {
        StatementParser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("amount", Some(SqlSumAggregation)))),
            condition = Some(Condition(NullableExpression(dimension = "creationDate"))),
            groupBy = Some("name"),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) shouldBe a[Failure[InvalidStatementException]]
      }

    }

    "executing computeRangeForInterval " should {
      "return a seq of ranges" in {
        val res = StatementParser.computeRangeForInterval(10L, 0L, 5L, Seq.empty)
        res.size shouldBe 2
        res.map(_.label).contains("5-10") shouldBe true
        res.map(_.label).contains("0-5") shouldBe true
      }
    }

    "executing computeRanges " should {
      "return a seq of ranges for a RangeExpression" in {
        val res = StatementParser.computeRanges(
          5L,
          None,
          Some(Condition(RangeExpression(dimension = "timestamp", value1 = 0L, value2 = 10L))),
          100L)
        res.size shouldBe 2
        res.map(_.label).contains("5-10") shouldBe true
        res.map(_.label).contains("0-5") shouldBe true
      }
      "return a seq of ranges for a left bounded interval(>=) lower than now " in {
        val res = StatementParser.computeRanges(
          5L,
          None,
          Some(
            Condition(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
          100L)
        res.size shouldBe 10
      }
      "return a seq of ranges for a right bounded interval(<=) lower than now " in {
        val res = StatementParser.computeRanges(
          5L,
          None,
          Some(
            Condition(
              ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 100L))),
          200L)

        res.exists(r => r.max == 99 && r.min == 95) shouldBe true
        res.size shouldBe 10
      }
      "return a seq of ranges for both right and left bounded interval( >= && <=) " in {
        val res = StatementParser.computeRanges(
          5L,
          None,
          Some(
            Condition(TupledLogicalExpression(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 70L),
              AndOperator,
              ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 100L)
            ))),
          200L
        )

        res.exists(r => r.max == 99 && r.min == 95) shouldBe true
        res.exists(r => r.max == 74 && r.min == 70) shouldBe true
        res.size shouldBe 6
      }

      "return a seq of ranges for both right and left bounded interval( > && <) " in {
        val res = StatementParser.computeRanges(
          5L,
          None,
          Some(
            Condition(TupledLogicalExpression(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 70L),
              AndOperator,
              ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 100L)
            ))),
          200L
        )

        res.exists(r => r.max == 94 && r.min == 90) shouldBe true
        res.exists(r => r.max == 74 && r.min == 70) shouldBe true
        res.size shouldBe 5
      }

    }
  }
}
