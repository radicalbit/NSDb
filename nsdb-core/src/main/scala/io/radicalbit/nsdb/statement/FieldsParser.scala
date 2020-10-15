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
import io.radicalbit.nsdb.model.Schema

object FieldsParser {

  /**
    * Checks if the field contains an aggregation on a field that is not the Value field
    */
  def aggregationNotOnValue(field: Field): Boolean =
    field.aggregation.isDefined && field.name != "value" && field.name != "*"

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
    lazy val requireTags: Boolean = list.exists(!_.aggregation.forall(_ == CountAggregation))
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
      case ListFields(List(singleField)) if aggregationNotOnValue(singleField) =>
        Left(StatementParserErrors.AGGREGATION_NOT_ON_VALUE)
      case ListFields(list) =>
        val metricDimensions = schema.fieldsMap.values.map(_.name).toSeq
        val projectionFields = list.map(_.name).filterNot(_ == "*")
        val diff             = projectionFields.filterNot(metricDimensions.contains)
        if (diff.isEmpty)
          Right(ParsedFields(list.map(f => SimpleField(f.name, f.aggregation))))
        else
          Left(StatementParserErrors.notExistingDimensions(diff))
      case ListFields(_) => Left(StatementParserErrors.AGGREGATION_NOT_ON_VALUE)
    }
}
