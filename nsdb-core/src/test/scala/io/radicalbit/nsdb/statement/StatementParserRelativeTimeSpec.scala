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
import io.radicalbit.nsdb.common.statement.SQLStatement._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.{Schema, TimeContext}
import io.radicalbit.nsdb.statement.FieldsParser.SimpleField
import io.radicalbit.nsdb.statement.StatementParser._
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search._
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration.Duration

class StatementParserRelativeTimeSpec extends WordSpec with Matchers {

  implicit val timeContext: TimeContext = TimeContext()

  private val schema = Schema(
    "people",
    Bit(0,
        1.1,
        dimensions = Map("name" -> "name", "surname" -> "surname", "creationDate" -> 0L),
        tags = Map("amount"     -> 1.1, "city"       -> "city", "country"         -> "country", "age" -> 0))
  )

  "A statement parser instance" when {

    "receive a select containing a range selection" should {
      "parse it successfully with both absolute and relative comparisons" in {
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
                                value2 = RelativeComparisonValue(plus, 4L, "min")))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", 2, timeContext.currentTime + Duration("4 min").toMillis),
              false,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
      "parse it successfully with relative comparisons" in {
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
                                value1 = RelativeComparisonValue(minus, 4L, "s"),
                                value2 = RelativeComparisonValue(plus, 4L, "min")))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp",
                                      timeContext.currentTime - Duration("4 s").toMillis,
                                      timeContext.currentTime + Duration("4 min").toMillis),
              false,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
    }

    "receive a select containing a GTE selection" should {
      "parse it successfully in case of relative comparison" in {
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
                                     value = RelativeComparisonValue(minus, 10L, "second")))),
            limit = Some(LimitOperator(4))
          ),
          schema
        ) should be(
          Right(
            ParsedSimpleQuery(
              "registry",
              "people",
              LongPoint.newRangeQuery("timestamp", timeContext.currentTime - Duration("10 s").toMillis, Long.MaxValue),
              false,
              4,
              List("name").map(SimpleField(_))
            ))
        )
      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "parse it successfully in case of relative comparisons" in {
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
                                                 value = RelativeComparisonValue(minus, 4L, "s")),
              operator = AndOperator,
              expression2 = ComparisonExpression(dimension = "timestamp",
                                                 comparison = LessOrEqualToOperator,
                                                 value = RelativeComparisonValue(plus, 4L, "hour"))
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
                .add(LongPoint.newRangeQuery("timestamp",
                                             timeContext.currentTime - Duration("4 s").toMillis + 1L,
                                             Long.MaxValue),
                     BooleanClause.Occur.MUST)
                .add(LongPoint.newRangeQuery("timestamp",
                                             Long.MinValue,
                                             timeContext.currentTime + Duration("4 h").toMillis),
                     BooleanClause.Occur.MUST)
                .build(),
              false,
              4,
              List("name").map(SimpleField(_))
            )
          ))
      }
    }
  }
}
