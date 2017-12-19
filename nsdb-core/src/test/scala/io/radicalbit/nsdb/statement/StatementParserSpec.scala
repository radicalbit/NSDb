package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.index.lucene.{MaxAllGroupsCollector, SumAllGroupsCollector}
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.statement.StatementParser.{ParsedAggregatedQuery, ParsedSimpleQuery}
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class StatementParserSpec extends WordSpec with Matchers {

  private val parser = new StatementParser

  val schema = Schema("people",
                      Seq(SchemaField("name", VARCHAR()),
                          SchemaField("surname", VARCHAR()),
                          SchemaField("creationDate", BIGINT()),
                          SchemaField("value", DECIMAL())))

  "A statement parser instance" when {

    "receive a select projecting a wildcard" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
                             fields = AllFields,
                             limit = Some(LimitOperator(4))),
          schema
        ) should be(
          Success(
            ParsedSimpleQuery(
              "registry",
              "people",
              new MatchAllDocsQuery(),
              4
            ))
        )
      }
    }

    "receive a select projecting a list of fields" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name", "surname", "creationDate")
            ))
        )
      }
    }

    "receive a select containing a range selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name")
            ))
        )
      }
    }

    "receive a select containing a = selection" should {
      "parse it successfully on a number vs a number" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name")
            ))
        )
      }
      "parse it successfully on a string vs a string" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name")
            ))
        )
      }
      "parse it successfully on a number vs a string" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name")
            ))
        )
      }
    }

    "receive a select containing a GTE selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name")
            ))
        )
      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name")
            )
          ))
      }
    }

    "receive a select containing a GTE OR a LT selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name")
            )
          )
        )
      }
    }

    "receive a select containing a GTE OR a = selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              4,
              List("name")
            )
          )
        )
      }
    }

    "receive a select containing a GTE OR a like selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
                    .add(new TermQuery(new Term("name", "*john*")), BooleanClause.Occur.SHOULD)
                    .build(),
                  BooleanClause.Occur.MUST_NOT
                )
                .build(),
              4,
              List("name")
            )
          )
        )
      }
    }

    "receive a select containing a ordering statement and a limit statement" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
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
              4,
              List.empty,
              Some(new Sort(new SortField("name", SortField.Type.STRING, false)))
            ))
        )
      }
    }

    "receive a select containing a ordering by value statement and a limit statement" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(db = "db",
                             namespace = "registry",
                             metric = "people",
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
              4,
              List.empty,
              Some(new Sort(new SortField("value", SortField.Type.DOUBLE, false)))
            ))
        )
      }
    }

    "receive a complex select containing a range selection a desc ordering statement and a limit statement" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
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
              5,
              List("name"),
              Some(new Sort(new SortField("creationDate", SortField.Type.LONG, true)))
            ))
        )
      }
    }

    "receive a statement without limit" should {
      "fail" in {
        parser.parseStatement(SelectSQLStatement(db = "db",
                                                 namespace = "registry",
                                                 metric = "people",
                                                 fields = AllFields),
                              schema) shouldBe 'failure
      }
    }

    "receive a select containing a range selection and a group by" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            fields = ListFields(List(Field("value", Some(SumAggregation)))),
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
              new SumAllGroupsCollector("name", "value")
            ))
        )
      }
    }

    "receive a complex select containing a range selection a desc ordering statement and a limit statement and a group by" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            db = "db",
            namespace = "registry",
            metric = "people",
            fields = ListFields(List(Field("value", Some(MaxAggregation)))),
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
            groupBy = Some("name"),
            order = Some(DescOrderOperator(dimension = "creationDate")),
            limit = Some(LimitOperator(5))
          ),
          schema
        ) should be(
          Success(
            ParsedAggregatedQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2L, 4L),
              new MaxAllGroupsCollector("name", "value"),
              Some(new Sort(new SortField("creationDate", SortField.Type.LONG, true))),
              Some(5)
            ))
        )
      }
    }
  }
}
