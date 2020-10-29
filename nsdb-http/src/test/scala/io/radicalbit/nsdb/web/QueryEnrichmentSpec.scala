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

package io.radicalbit.nsdb.web

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.web.Filters._
import io.radicalbit.nsdb.test.NSDbSpec

class QueryEnrichmentSpec extends NSDbSpec {

  "QueryEnrichment " when {

    "Query with a single filter over a dimension of Long type" should {
      "be correctly converted with equal operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.Equality))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(EqualityExpression("age", AbsoluteComparisonValue(1L)))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with GT operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.GreaterThan))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(ComparisonExpression("age", GreaterThanOperator, AbsoluteComparisonValue(1L)))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with GTE operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.GreaterOrEqual))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(ComparisonExpression("age", GreaterOrEqualToOperator, AbsoluteComparisonValue(1L)))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with LT operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.LessThan))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(ComparisonExpression("age", LessThanOperator, AbsoluteComparisonValue(1L)))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with LTE operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.LessOrEqual))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(ComparisonExpression("age", LessOrEqualToOperator, AbsoluteComparisonValue(1L)))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
    }

    "Query with a single filter over a dimension of String type" should {
      "be correctly converted with equal operator" in {
        val filters = Seq(FilterByValue("surname", "Poe", FilterOperators.Equality))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(EqualityExpression("surname", AbsoluteComparisonValue("Poe")))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with LIKE operator" in {
        val filters = Seq(FilterByValue("surname", "Poe", FilterOperators.Like))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(LikeExpression("surname", "Poe"))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
    }

    "Query with multiple filters of different types" should {
      "be correctly converted with equal operators" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.Equality),
                          FilterByValue("height", 100L, FilterOperators.Equality))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(EqualityExpression("age", AbsoluteComparisonValue(1L)),
                                        AndOperator,
                                        EqualityExpression("height", AbsoluteComparisonValue(100L))))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with equal operators and existing Condition" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.Equality),
                          FilterByValue("height", 100L, FilterOperators.Equality))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          Some(Condition(EqualityExpression("surname", AbsoluteComparisonValue("poe")))),
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(
                  EqualityExpression("surname", AbsoluteComparisonValue("poe")),
                  AndOperator,
                  TupledLogicalExpression(
                    EqualityExpression("age", AbsoluteComparisonValue(1L)),
                    AndOperator,
                    EqualityExpression("height", AbsoluteComparisonValue(100L))
                  )
                )
              )),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with different operators and existing Condition" in {
        val filters =
          Seq(FilterByValue("age", 1L, FilterOperators.GreaterThan),
              FilterByValue("height", 100L, FilterOperators.LessOrEqual))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          Some(Condition(LikeExpression("surname", "poe"))),
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(
                  LikeExpression("surname", "poe"),
                  AndOperator,
                  TupledLogicalExpression(
                    ComparisonExpression("age", GreaterThanOperator, AbsoluteComparisonValue(1L)),
                    AndOperator,
                    ComparisonExpression("height", LessOrEqualToOperator, AbsoluteComparisonValue(100L))
                  )
                )
              )),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with different operators and existing Conditions" in {
        val filters =
          Seq(FilterByValue("age", 1L, FilterOperators.GreaterThan),
              FilterByValue("height", 100L, FilterOperators.LessOrEqual))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          Some(
            Condition(
              TupledLogicalExpression(LikeExpression("surname", "poe"),
                                      OrOperator,
                                      EqualityExpression("number", AbsoluteComparisonValue(1.0))))),
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(
                  TupledLogicalExpression(LikeExpression("surname", "poe"),
                                          OrOperator,
                                          EqualityExpression("number", AbsoluteComparisonValue(1.0))),
                  AndOperator,
                  TupledLogicalExpression(
                    ComparisonExpression("age", GreaterThanOperator, AbsoluteComparisonValue(1L)),
                    AndOperator,
                    ComparisonExpression("height", LessOrEqualToOperator, AbsoluteComparisonValue(100L))
                  )
                )
              )),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
      "be correctly converted with different operators (also Not) and existing Conditions" in {
        val filters =
          Seq(FilterByValue("age", 1L, FilterOperators.GreaterThan),
              FilterByValue("height", 100L, FilterOperators.LessOrEqual))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          Some(
            Condition(
              TupledLogicalExpression(LikeExpression("surname", "poe"),
                                      OrOperator,
                                      NotExpression(EqualityExpression("number", AbsoluteComparisonValue(1.0)))))),
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(
                  TupledLogicalExpression(LikeExpression("surname", "poe"),
                                          OrOperator,
                                          NotExpression(EqualityExpression("number", AbsoluteComparisonValue(1.0)))),
                  AndOperator,
                  TupledLogicalExpression(
                    ComparisonExpression("age", GreaterThanOperator, AbsoluteComparisonValue(1L)),
                    AndOperator,
                    ComparisonExpression("height", LessOrEqualToOperator, AbsoluteComparisonValue(100L))
                  )
                )
              )),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
    }

    "Query with a single filter of nullable conditions" should {
      "be correctly converted with is null" in {
        val filters = Seq(FilterNullableValue("age", NullableOperators.IsNull))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(NullableExpression("age"))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }

      "be correctly converted with is not null" in {
        val filters = Seq(FilterNullableValue("age", NullableOperators.IsNotNull))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(Condition(NotExpression(NullableExpression("age")))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }

      "be correctly converted with is null and is not null" in {
        val filters = Seq(FilterNullableValue("age", NullableOperators.IsNull),
                          FilterNullableValue("height", NullableOperators.IsNotNull))
        val originalStatement = SelectSQLStatement(
          db = "db",
          namespace = "namespace",
          metric = "people",
          distinct = false,
          fields = ListFields(List(Field("name", None))),
          condition = None,
          groupBy = None,
          order = None,
          limit = Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filter.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "people",
            distinct = false,
            fields = ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(NullableExpression("age"),
                                        AndOperator,
                                        NotExpression(NullableExpression("height"))))),
            groupBy = None,
            order = None,
            limit = Some(LimitOperator(1))
          )
      }
    }

  }
}
