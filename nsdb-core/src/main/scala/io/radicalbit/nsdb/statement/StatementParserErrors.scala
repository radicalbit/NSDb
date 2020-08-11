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

object StatementParserErrors {

  lazy val NO_AGGREGATION_GROUP_BY = "cannot execute a groupField by query without an aggregation"
  lazy val MORE_FIELDS_GROUP_BY    = "cannot execute a groupField by query with more than a aggregateField"
  lazy val MORE_FIELDS_DISTINCT    = "cannot execute a select distinct projecting more than one field"
  lazy val NO_GROUP_BY_AGGREGATION =
    s"cannot execute a query with that aggregation without a groupBy field"
  lazy val GROUP_BY_DISTINCT =
    s"cannot execute a query with a group by and a distinct clause"
  def TAGLESS_AGGREGATION(aggregationType: String) =
    s"cannot execute a query with $aggregationType aggregation for a metric with no tags defined  "
  lazy val SIMPLE_AGGREGATION_NOT_ON_TAG =
    "cannot execute a groupBy query grouping by a field that is not a tag"
  lazy val AGGREGATION_NOT_ON_VALUE =
    "cannot execute an aggregation on a field different than value"
  lazy val SORT_DIMENSION_NOT_IN_GROUP =
    "cannot sort group by query result by a field not in group by clause"
  def notExistingDimension(dim: String)       = s"dimension $dim does not exist"
  def notExistingDimensions(dim: Seq[String]) = s"dimensions [${dim.mkString(",")}] does not exist"
  def uncompatibleOperator(operator: String, dimTypeAllowed: String) =
    s"cannot use $operator operator on dimension different from $dimTypeAllowed"

}
