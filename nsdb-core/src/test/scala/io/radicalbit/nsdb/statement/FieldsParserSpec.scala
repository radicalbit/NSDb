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
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.statement.FieldsParser.{ParsedFields, SimpleField}
import org.scalatest.{Matchers, WordSpec}

class FieldsParserSpec extends WordSpec with Matchers {

  private val schema = Schema(
    "people",
    Bit(0,
        1.1,
        dimensions = Map("name" -> "name", "surname" -> "surname", "creationDate" -> 0L),
        tags = Map("amount"     -> 1.1, "city"       -> "city", "country"         -> "country", "age" -> 0))
  )

  "FieldsParser" should {

    "parse a wildcard projection" in {
      FieldsParser.parseFieldList(AllFields(), schema) shouldBe Right(ParsedFields(List.empty))
    }

    "parse allowed aggregation on tag" in {
      FieldsParser.parseFieldList(ListFields(List(Field("amount", Some(CountAggregation("amount"))))), schema) shouldBe
        Right(ParsedFields(List(SimpleField("amount", Some(CountAggregation("amount"))))))

      FieldsParser.parseFieldList(ListFields(List(Field("amount", Some(CountDistinctAggregation("amount"))))), schema) shouldBe
        Right(ParsedFields(List(SimpleField("amount", Some(CountDistinctAggregation("amount"))))))
    }

    "reject fields containing not allowed aggregations on tags" in {
      FieldsParser.parseFieldList(ListFields(List(Field("country", Some(SumAggregation("country"))))), schema) shouldBe Left(
        StatementParserErrors.AGGREGATION_NOT_ALLOWED)

      FieldsParser.parseFieldList(ListFields(List(Field("country", Some(AvgAggregation("amount"))))), schema) shouldBe Left(
        StatementParserErrors.AGGREGATION_NOT_ALLOWED)

      FieldsParser.parseFieldList(ListFields(List(Field("country", Some(MinAggregation("country"))))), schema) shouldBe Left(
        StatementParserErrors.AGGREGATION_NOT_ALLOWED)

      FieldsParser.parseFieldList(ListFields(List(Field("country", Some(FirstAggregation("amount"))))), schema) shouldBe Left(
        StatementParserErrors.AGGREGATION_NOT_ALLOWED)

    }

    "reject fields containing multiple count or count distinct" in {
      FieldsParser.parseFieldList(
        ListFields(
          List(Field("*", Some(CountAggregation("value"))), Field("amount", Some(CountAggregation("amount"))))),
        schema) shouldBe Left(StatementParserErrors.MULTIPLE_COUNT_AGGREGATIONS)

      FieldsParser.parseFieldList(ListFields(
                                    List(Field("*", Some(CountDistinctAggregation("value"))),
                                         Field("amount", Some(CountDistinctAggregation("amount"))))),
                                  schema) shouldBe Left(StatementParserErrors.MULTIPLE_COUNT_AGGREGATIONS)

      FieldsParser.parseFieldList(
        ListFields(
          List(Field("*", Some(CountAggregation("value"))),
               Field("amount", Some(CountAggregation("amount"))),
               Field("*", Some(CountDistinctAggregation("value"))))),
        schema
      ) shouldBe Left(StatementParserErrors.MULTIPLE_COUNT_AGGREGATIONS)

      FieldsParser.parseFieldList(
        ListFields(
          List(
            Field("*", Some(CountDistinctAggregation("value"))),
            Field("amount", Some(CountDistinctAggregation("amount"))),
            Field("*", Some(CountAggregation("value")))
          )),
        schema
      ) shouldBe Left(StatementParserErrors.MULTIPLE_COUNT_AGGREGATIONS)
    }

    "reject fields containing not allowed aggregations on dimensions" in {
      FieldsParser.parseFieldList(ListFields(List(Field("name", Some(CountAggregation("name"))))), schema) shouldBe Left(
        StatementParserErrors.AGGREGATION_NOT_ALLOWED)

      FieldsParser.parseFieldList(ListFields(List(Field("name", Some(CountDistinctAggregation("name"))))), schema) shouldBe Left(
        StatementParserErrors.AGGREGATION_NOT_ALLOWED)
    }

    "reject fields not existing in the schema" in {
      FieldsParser.parseFieldList(ListFields(List(Field("notExisting", None))), schema) shouldBe Left(
        StatementParserErrors.notExistingFields(Seq("notExisting")))
    }
  }

  "ParsedFields" should {
    "compute requireTags true if only standard aggregations are provided" in {
      ParsedFields(List(SimpleField("*", Some(SumAggregation("value"))),
                        SimpleField("*", Some(AvgAggregation("value"))))).requireTags shouldBe true
    }

    "compute requireTags true if mixed count and standard aggregations are provided" in {
      ParsedFields(List(SimpleField("*", Some(CountAggregation("value"))),
                        SimpleField("*", Some(AvgAggregation("value"))))).requireTags shouldBe true
    }

    "compute requireTags false if only plain fields are provided" in {
      ParsedFields(List(SimpleField("field1"), SimpleField("field2"))).requireTags shouldBe false
    }

    "compute requireTags false if mixed count and plain fields are provided" in {
      ParsedFields(List(SimpleField("field1", Some(CountAggregation("field1"))), SimpleField("field2"))).requireTags shouldBe false
    }
  }
}
