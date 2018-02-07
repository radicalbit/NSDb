package io.radicalbit.nsdb.web

import io.radicalbit.nsdb.common.statement._
import org.scalatest.{Matchers, WordSpec}

class QueryEnrichmentTest extends WordSpec with Matchers {

  "QueryEnrichment " when {

    "Query with a single filter over a dimension of Long type" should {
      "be correctly converted with equal operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.Equality))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement("db",
                             "namespace",
                             "people",
                             false,
                             ListFields(List(Field("name", None))),
                             Some(Condition(EqualityExpression("age", 1L))),
                             None,
                             None,
                             Some(LimitOperator(1)))
      }
      "be correctly converted with GT operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.GreaterThan))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(Condition(ComparisonExpression("age", GreaterThanOperator, 1L))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
      "be correctly converted with GTE operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.GreaterOrEqual))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(Condition(ComparisonExpression("age", GreaterOrEqualToOperator, 1L))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
      "be correctly converted with LT operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.LessThan))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(Condition(ComparisonExpression("age", LessThanOperator, 1L))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
      "be correctly converted with LTE operator" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.LessOrEqual))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(Condition(ComparisonExpression("age", LessOrEqualToOperator, 1L))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
    }

    "Query with a single filter over a dimension of String type" should {
      "be correctly converted with equal operator" in {
        val filters = Seq(FilterByValue("surname", "Poe", FilterOperators.Equality))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(Condition(EqualityExpression("surname", "Poe"))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
      "be correctly converted with LIKE operator" in {
        val filters = Seq(FilterByValue("surname", "Poe", FilterOperators.Like))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(Condition(LikeExpression("surname", "Poe"))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
    }

    "Query with multiple filters of different types" should {
      "be correctly converted with equal operators" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.Equality),
                          FilterByValue("height", 100L, FilterOperators.Equality))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(EqualityExpression("age", 1L),
                                        AndOperator,
                                        EqualityExpression("height", 100L)))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
      "be correctly converted with equal operators and existing Condition" in {
        val filters = Seq(FilterByValue("age", 1L, FilterOperators.Equality),
                          FilterByValue("height", 100L, FilterOperators.Equality))
        val originalStatement = SelectSQLStatement(
          "db",
          "namespace",
          "people",
          false,
          ListFields(List(Field("name", None))),
          Some(Condition(EqualityExpression("surname", "poe"))),
          None,
          None,
          Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(
                  EqualityExpression("surname", "poe"),
                  AndOperator,
                  TupledLogicalExpression(
                    EqualityExpression("age", 1L),
                    AndOperator,
                    EqualityExpression("height", 100L)
                  )
                )
              )),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
      "be correctly converted with different operators and existing Condition" in {
        val filters =
          Seq(FilterByValue("age", 1L, FilterOperators.GreaterThan),
              FilterByValue("height", 100L, FilterOperators.LessOrEqual))
        val originalStatement = SelectSQLStatement(
          "db",
          "namespace",
          "people",
          false,
          ListFields(List(Field("name", None))),
          Some(Condition(LikeExpression("surname", "poe"))),
          None,
          None,
          Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(
                  LikeExpression("surname", "poe"),
                  AndOperator,
                  TupledLogicalExpression(
                    ComparisonExpression("age", GreaterThanOperator, 1L),
                    AndOperator,
                    ComparisonExpression("height", LessOrEqualToOperator, 100L)
                  )
                )
              )),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
      "be correctly converted with different operators and existing Conditions" in {
        val filters =
          Seq(FilterByValue("age", 1L, FilterOperators.GreaterThan),
              FilterByValue("height", 100L, FilterOperators.LessOrEqual))
        val originalStatement = SelectSQLStatement(
          "db",
          "namespace",
          "people",
          false,
          ListFields(List(Field("name", None))),
          Some(Condition(
            TupledLogicalExpression(LikeExpression("surname", "poe"), OrOperator, EqualityExpression("number", 1.0)))),
          None,
          None,
          Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(
                  TupledLogicalExpression(LikeExpression("surname", "poe"),
                                          OrOperator,
                                          EqualityExpression("number", 1.0)),
                  AndOperator,
                  TupledLogicalExpression(
                    ComparisonExpression("age", GreaterThanOperator, 1L),
                    AndOperator,
                    ComparisonExpression("height", LessOrEqualToOperator, 100L)
                  )
                )
              )),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
      "be correctly converted with different operators (also Not) and existing Conditions" in {
        val filters =
          Seq(FilterByValue("age", 1L, FilterOperators.GreaterThan),
              FilterByValue("height", 100L, FilterOperators.LessOrEqual))
        val originalStatement = SelectSQLStatement(
          "db",
          "namespace",
          "people",
          false,
          ListFields(List(Field("name", None))),
          Some(
            Condition(
              TupledLogicalExpression(LikeExpression("surname", "poe"),
                                      OrOperator,
                                      UnaryLogicalExpression(EqualityExpression("number", 1.0), NotOperator)))),
          None,
          None,
          Some(LimitOperator(1))
        )

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(
                  TupledLogicalExpression(LikeExpression("surname", "poe"),
                                          OrOperator,
                                          UnaryLogicalExpression(EqualityExpression("number", 1.0), NotOperator)),
                  AndOperator,
                  TupledLogicalExpression(
                    ComparisonExpression("age", GreaterThanOperator, 1L),
                    AndOperator,
                    ComparisonExpression("height", LessOrEqualToOperator, 100L)
                  )
                )
              )),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
    }

    "Query with a single filter of nullable conditions" should {
      "be correctly converted with is null" in {
        val filters = Seq(FilterNullableValue("age", CheckOperators.IsNull))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(Condition(NullableExpression("age"))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }

      "be correctly converted with is not null" in {
        val filters = Seq(FilterNullableValue("age", CheckOperators.IsNotNull))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(Condition(UnaryLogicalExpression(NullableExpression("age"), NotOperator))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }

      "be correctly converted with is null and is not null" in {
        val filters = Seq(FilterNullableValue("age", CheckOperators.IsNull),
                          FilterNullableValue("height", CheckOperators.IsNotNull))
        val originalStatement = SelectSQLStatement("db",
                                                   "namespace",
                                                   "people",
                                                   false,
                                                   ListFields(List(Field("name", None))),
                                                   None,
                                                   None,
                                                   None,
                                                   Some(LimitOperator(1)))

        val enrichedStatement = originalStatement.addConditions(filters.map(Filters.unapply(_).get))

        enrichedStatement shouldEqual
          SelectSQLStatement(
            "db",
            "namespace",
            "people",
            false,
            ListFields(List(Field("name", None))),
            Some(
              Condition(
                TupledLogicalExpression(NullableExpression("age"),
                                        AndOperator,
                                        UnaryLogicalExpression(NullableExpression("height"), NotOperator)))),
            None,
            None,
            Some(LimitOperator(1))
          )
      }
    }

  }
}
