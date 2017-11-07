package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.statement._
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class AggregationSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A parser instance" when {

    "receive a select with a group by and one aggregation" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry", input = "SELECT sum(value) FROM people group by name") should be(
          Success(
            SelectSQLStatement(namespace = "registry",
                               metric = "people",
                               fields = ListFields(List(Field("value", Some(SumAggregation)))),
                               groupBy = Some("name"))
          ))
      }
    }

    "receive a select containing a range selection and a group by" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry",
                     input = "SELECT count(value) FROM people WHERE timestamp IN (2,4) group by name") should be(
          Success(SelectSQLStatement(
            namespace = "registry",
            metric = "people",
            fields = ListFields(List(Field("value", Some(CountAggregation)))),
            condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
            groupBy = Some("name")
          )))
      }
    }

    "receive a select containing a GTE selection and a group by" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry",
                     input = "SELECT min(value) FROM people WHERE timestamp >= 10 group by name") should be(
          Success(SelectSQLStatement(
            namespace = "registry",
            metric = "people",
            fields = ListFields(List(Field("value", Some(MinAggregation)))),
            condition = Some(Condition(
              ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
            groupBy = Some("name")
          )))
      }
    }

    "receive a select containing a GT AND a LTE selection and a group by" should {
      "parse it successfully" in {
        parser.parse(
          namespace = "registry",
          input = "SELECT max(value) FROM people WHERE timestamp > 2 AND timestamp <= 4 group by name") should be(
          Success(SelectSQLStatement(
            namespace = "registry",
            metric = "people",
            fields = ListFields(List(Field("value", Some(MaxAggregation)))),
            condition = Some(Condition(TupledLogicalExpression(
              expression1 = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = AndOperator,
              expression2 =
                ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
            ))),
            groupBy = Some("name")
          )))
      }
    }

    "receive a select containing a ordering statement" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry", input = "SELECT count(value) FROM people group by name ORDER BY name") should be(
          Success(SelectSQLStatement(
            namespace = "registry",
            metric = "people",
            fields = ListFields(List(Field("value", Some(CountAggregation)))),
            order = Some(AscOrderOperator("name")),
            groupBy = Some("name")
          )))
      }
    }

    "receive a select containing a ordering statement and a limit clause" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry",
                     input = "SELECT count(value) FROM people group by name ORDER BY name limit 1") should be(
          Success(SelectSQLStatement(
            namespace = "registry",
            metric = "people",
            fields = ListFields(List(Field("value", Some(CountAggregation)))),
            order = Some(AscOrderOperator("name")),
            groupBy = Some("name"),
            limit = Some(LimitOperator(1))
          )))
      }
    }

    "receive wrong fields" should {
      "fail" in {
        parser.parse(namespace = "registry", input = "SELECT count(name), min(surname) FROM people") shouldBe 'failure
        parser.parse(namespace = "registry", input = "SELECT count(name,surname) FROM people") shouldBe 'failure
      }
    }
  }
}
