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
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.web.routes.Filter

import scala.util.Success

object QueryEnriched {

  def apply(db: String,
            namespace: String,
            queryString: String,
            from: Option[Long],
            to: Option[Long],
            filters: Seq[Filter]) =
    (new SQLStatementParser().parse(db, namespace, queryString), from, to, filters) match {
      case (Success(statement: SelectSQLStatement), Some(from), Some(to), filters) if filters.nonEmpty =>
        Some(
          statement
            .enrichWithTimeRange("timestamp", from, to)
            .addConditions(filters.map(f => Filter.unapply(f).get)))
      case (Success(statement: SelectSQLStatement), None, None, filters) if filters.nonEmpty =>
        Some(
          statement
            .addConditions(filters.map(f => Filter.unapply(f).get)))
      case (Success(statement: SelectSQLStatement), Some(from), Some(to), _) =>
        Some(
          statement
            .enrichWithTimeRange("timestamp", from, to))
      case (Success(statement: SelectSQLStatement), _, _, _) =>
        Some(statement)
      case _ => None
    }

}
