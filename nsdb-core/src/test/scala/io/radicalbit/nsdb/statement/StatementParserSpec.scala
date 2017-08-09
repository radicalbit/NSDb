package io.radicalbit.nsdb.statement

import io.radicalbit.nsdb.statement.StatementParser.QueryResult
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search._
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class StatementParserSpec extends WordSpec with Matchers {

  private val parser = new StatementParser

  "A statement parser instance" when {

    "receive a select projecting a wildcard" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(metric = "people", fields = AllFields, limit = Some(LimitOperator(4)))
        ) should be(
          Success(
            QueryResult(
              new MatchAllDocsQuery(),
              4
            ))
        )
      }
    }

    "receive a select projecting a list of fields" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(metric = "people",
                             fields = ListFields(List("name", "surname", "creationDate")),
                             limit = Some(LimitOperator(4)))
        ) should be(
          Success(
            QueryResult(
              new MatchAllDocsQuery(),
              4
            ))
        )
      }
    }

    "receive a select containing a range selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            metric = "people",
            fields = ListFields(List("name")),
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
            limit = Some(LimitOperator(4))
          )
        ) should be(
          Success(
            QueryResult(
              LongPoint.newRangeQuery("timestamp", 2, 4),
              4
            ))
        )
      }
    }

    "receive a select containing a GTE selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            metric = "people",
            fields = ListFields(List("name")),
            condition = Some(Condition(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
            limit = Some(LimitOperator(4))
          )
        ) should be(
          Success(
            QueryResult(
              LongPoint.newRangeQuery("timestamp", 10L, Long.MaxValue),
              4
            ))
        )
      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            metric = "people",
            fields = ListFields(List("name")),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = AndOperator,
              expression2 =
                ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
            ))),
            limit = Some(LimitOperator(4))
          )
        ) should be(
          Success(
            QueryResult(
              new BooleanQuery.Builder()
                .add(LongPoint.newRangeQuery("timestamp", 2L + 1, Long.MaxValue), BooleanClause.Occur.MUST)
                .add(LongPoint.newRangeQuery("timestamp", 0, 4L), BooleanClause.Occur.MUST)
                .build(),
              4
            )
          ))
      }
    }

    "receive a select containing a GTE OR a LT selection" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(
            metric = "people",
            fields = ListFields(List("name")),
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
          )
        ) should be(
          Success(
            QueryResult(
              new BooleanQuery.Builder()
                .add(
                  new BooleanQuery.Builder()
                    .add(LongPoint.newRangeQuery("timestamp", 2L, Long.MaxValue), BooleanClause.Occur.SHOULD)
                    .add(LongPoint.newRangeQuery("timestamp", 0, 4L - 1), BooleanClause.Occur.SHOULD)
                    .build(),
                  BooleanClause.Occur.MUST_NOT
                )
                .build(),
              4
            )
          )
        )
      }
    }

    "receive a select containing a ordering statement" should {
      "parse it successfully" in {
        parser.parseStatement(
          SelectSQLStatement(metric = "people",
                             fields = AllFields,
                             order = Some(AscOrderOperator("name")),
                             limit = Some(LimitOperator(4)))
        ) should be(
          Success(
            QueryResult(
              new MatchAllDocsQuery(),
              4,
              Some(new Sort(new SortField("name", SortField.Type.DOC)))
            ))
        )
      }
    }

//    "receive a complex select containing a range selection a desc ordering statement and a limit statement" should {
//      "parse it successfully" in {
//        parser.parse("SELECT name FROM people WHERE timestamp IN (2,4) ORDER BY name DESC LIMIT 5") should be(
//          Success(SelectSQLStatement(
//            metric = "people",
//            fields = ListFields(List("name")),
//            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = "2", value2 = "4"))),
//            order = Some(DescOrderOperator(dimension = "name")),
//            limit = Some(LimitOperator(5))
//          )))
//      }
//    }

    "receive a statement withoud limit" should {
      "fail" in {
        parser.parseStatement(SelectSQLStatement(metric = "people", fields = AllFields)) shouldBe 'failure
      }
    }
  }
}
