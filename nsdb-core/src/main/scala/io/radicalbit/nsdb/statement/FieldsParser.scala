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

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.{Schema, SchemaField}

object FieldsParser {

  /**
    * Checks if the field contains an aggregation on a field that is not the Value field
    */
  def aggregationNotAllowed(field: Field, tags: Map[String, SchemaField]): Boolean = {

    val allowedAggregationOnTag = field.aggregation.exists(a =>
      a.isInstanceOf[CountAggregation] || a.isInstanceOf[CountDistinctAggregation]) && tags
      .contains(field.name)

    field.aggregation.isDefined && field.name != "value" && field.name != "*" && !allowedAggregationOnTag
  }

  /**
    * Checks if a list of fields contains at least one standard aggregation.
    */
  def containsStandardAggregations(fields: List[SimpleField]): Boolean =
    fields.exists(f => f.aggregation.exists(!_.isInstanceOf[GlobalAggregation]))

  /**
    * Checks if the aggregation for a field (if exists) is global
    */
  def containsOnlyGlobalAggregations(fields: List[SimpleField]): Boolean =
    fields.exists(f => f.aggregation.exists(_.isInstanceOf[GlobalAggregation]))

  private[statement] case class ParsedFields(list: List[SimpleField]) {
    lazy val requireTags: Boolean = list.exists(!_.aggregation.forall(_.isInstanceOf[CountAggregation]))
  }

  /**
    * Simple query field.
    *
    * @param name  field to be returned into query results.
    */
  case class SimpleField(name: String, aggregation: Option[Aggregation] = None)

  /**
    * Parses and validates the SQL fields list.
    * The following checks are performed
    * - All fields must be present in the metric schema.
    * - All aggregation must be against value or *.
    * @param sqlFields the SQL statement fields.
    * @param schema the metric schema.
    */
  def parseFieldList(sqlFields: SelectedFields, schema: Schema): Either[String, ParsedFields] =
    sqlFields match {
      case AllFields() => Right(ParsedFields(List.empty))
      case ListFields(List(singleField)) if aggregationNotAllowed(singleField, schema.tags) =>
        Left(StatementParserErrors.AGGREGATION_NOT_ALLOWED)
      case ListFields(list) =>
        val metricDimensions = schema.fieldsMap.values.map(_.name).toSeq
        val projectionFields = list.map(_.name).filterNot(_ == "*")
        val diff             = projectionFields.filterNot(metricDimensions.contains)
        if (diff.isEmpty)
          Right(ParsedFields(list.map(f => SimpleField(f.name, f.aggregation))))
        else
          Left(StatementParserErrors.notExistingDimensions(diff))
      case ListFields(_) => Left(StatementParserErrors.AGGREGATION_NOT_ALLOWED)
    }
}
