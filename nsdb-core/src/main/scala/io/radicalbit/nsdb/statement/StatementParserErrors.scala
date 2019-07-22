/*
 * Copyright 2018 Radicalbit S.r.l.
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
  lazy val MORE_FIELDS_DISTINCT    = "cannot execute a select distinct projecting more than one dimension"
  lazy val NO_GROUP_BY_AGGREGATION =
    "cannot execute a query with aggregation different than count without a groupField by"
  lazy val AGGREGATION_NOT_ON_VALUE =
    "cannot execute a groupField by query performing an aggregation on dimension different from value"
  lazy val SORT_DIMENSION_NOT_IN_GROUP =
    "cannot sort groupField by query result by a dimension not in groupField"
  lazy val NOT_SUPPORTED_AGGREGATION_IN_TEMPORAL_GROUP_BY =
    "not supported aggregation function for temporal grouping query"
  def notExistingDimension(dim: String)       = s"dimension $dim does not exist"
  def notExistingDimensions(dim: Seq[String]) = s"dimensions [$dim] does not exist"
  def uncompatibleOperator(operator: String, dimTypeAllowed: String) =
    s"cannot use $operator operator on dimension different from $dimTypeAllowed"

}
