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

import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import io.radicalbit.nsdb.sql.parser._
import io.radicalbit.nsdb.web.Filters.Filter

/**
  * Enriches an input query with:
  * - a time range (from and to)
  * - a sequence of [[Filter]]
  */
object QueryEnriched {

  /**
    * apply enrichment process (if provided)
    * @param db the db.
    * @param namespace the namespace.
    * @param inputQueryString the raw query string.
    * @param from the lower time range boundary.
    * @param to the upper time range boundary.
    * @param filters a sequence of [[Filter]].
    * @return the [[SqlStatementParserResult]] containing the enriched statement.
    *         If no time range or filters are provided the original statement is returned.
    */
  def apply(db: String,
            namespace: String,
            inputQueryString: String,
            from: Option[Long] = None,
            to: Option[Long] = None,
            filters: Seq[Filter] = Seq.empty): SqlStatementParserResult =
    (new SQLStatementParser().parse(db, namespace, inputQueryString), from, to, filters) match {
      case (SqlStatementParserSuccess(queryString, statement: SelectSQLStatement), Some(from), Some(to), filters)
          if filters.nonEmpty =>
        SqlStatementParserSuccess(queryString,
                                  statement
                                    .enrichWithTimeRange("timestamp", from, to)
                                    .addConditions(filters.map(f => Filter.unapply(f).get)))
      case (SqlStatementParserSuccess(queryString, statement: SelectSQLStatement), None, None, filters)
          if filters.nonEmpty =>
        SqlStatementParserSuccess(queryString,
                                  statement
                                    .addConditions(filters.map(f => Filter.unapply(f).get)))
      case (SqlStatementParserSuccess(queryString, statement: SelectSQLStatement), Some(from), Some(to), _) =>
        SqlStatementParserSuccess(queryString,
                                  statement
                                    .enrichWithTimeRange("timestamp", from, to))
      case (success @ SqlStatementParserSuccess(_, _: SelectSQLStatement), _, _, _) => success
      case (SqlStatementParserSuccess(queryString, _), _, _, _) =>
        SqlStatementParserFailure(queryString, "not a select statement")
      case (failure: SqlStatementParserFailure, _, _, _) => failure
    }

}
