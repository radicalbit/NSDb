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

package io.radicalbit.nsdb.cli.table

import java.time.{Instant, LocalDateTime}
import java.util.TimeZone

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import de.vandermeer.asciitable.{AsciiTable, CWC_LongestWord}
import de.vandermeer.asciithemes.a7.A7_Grids
import io.radicalbit.nsdb.common.protocol._
import scala.concurrent.duration._

import scala.util.Try

/**
  * Object used to render server responses on scala REPL.
  * It provides a table representation of query and command results making use of [[de.vandermeer.asciitable.AsciiTable]].
  *
  * @param tableMaxWidth Defines table maxWidth, if the number of char of a single line is higher than this value,
  *                      just a fixed number of dimension / tag is rendered.
  */
class ASCIITableBuilder(tableMaxWidth: Int) extends LazyLogging {

  type FieldName = String
  type Row       = List[String]

  /**
    * Defines the number of fields rendered in case of table exceeding tableMaxWidth
    * It doesn't take count fo timestamp and value fixed columns
    */
  private val fieldLimit = 3

  /**
    * Extract ColumnNames from a successful [[SQLStatementExecuted]]
    * @param stm sql statement response
    * @return [[Map]] representation of [[Bit]] dimensions and tags
    */
  private def extractColumnNames(stm: SQLStatementExecuted): Map[FieldName, Option[String]] =
    stm.res
      .flatMap(bit =>
        (bit.dimensions ++ bit.tags).map {
          case (name, _) if name.trim.length > 0 => (name.trim, None)
      })
      .toMap

  /**
    * Renders [[SQLStatementResult]] results in a table format
    *
    * @param stm [[SQLStatementResult]] containing headers and rows to be rendered in table
    * @return [[String]] table
    */
  def tableFor(stm: SQLStatementResult): Try[String] =
    stm match {
      case statement: SQLStatementExecuted if statement.res.nonEmpty =>
        Try {
          val allFields: Map[FieldName, Option[String]] = extractColumnNames(statement)

          val rows: List[Row] = statement.res.toList.map { x =>
            val fieldsMap    = (x.dimensions ++ x.tags).map { case (k, v)                 => (k, Some(v.toString)) }
            val mergedFields = allFields.combine(fieldsMap).toList.sortBy { case (col, _) => col }
            // prepending timestamp and value

            LocalDateTime
              .ofInstant(Instant.ofEpochMilli(x.timestamp), TimeZone.getTimeZone("UTC").toZoneId)
              .toString +: x.value.toString +: mergedFields.map {
              case (_, value) => value getOrElse ""
            }
          }

          val maxFieldNumber = allFields.keys.size

          // Find max num of char for a file, if so omit some fields from rendering
          if (Math.max(allFields.keys.foldLeft(0)((acc, k) => acc + k.length),
                       allFields.values.flatten.foldLeft(0)((acc, k) => acc + k.length)) < tableMaxWidth)
            render("timestamp" +: "value" +: allFields.toList.map(_._1).sorted, rows)
          else {
            // Add a new column containing as header the number of omitted fields
            val nMoreFields   = maxFieldNumber - fieldLimit
            val summaryHeader = List(s"$nMoreFields more fields")
            val headers = List("timestamp", "value") ++ allFields.toList
              .map(_._1)
              .sorted
              .take(fieldLimit) ++ summaryHeader
            // take 2 more columns for timestamp and value, add one more columns for summary of remaining fields
            val newRows = rows.map(column => column.take(fieldLimit + 2) ++ List(""))
            render(headers, newRows)
          }

        }

      case statement: SQLStatementExecuted if statement.res.isEmpty =>
        Try("Statement executed successfully, no records to display")

      case failStatement: SQLStatementFailed =>
        Try(failStatement.reason)

    }

  /**
    * Renders [[CommandStatementExecuted]] results in a table format
    *
    * @param commandResult [[CommandStatementExecuted]] containing server results for a command
    * @return [[String]] table
    */
  def tableFor(commandResult: CommandStatementExecuted): Try[String] = {
    commandResult match {
      case res: NamespaceMetricsListRetrieved =>
        Try(render(List("Metric Name"), res.metrics.map(m => List(m))))
      case res: DescribeMetricResponse =>
        Try(
          StringBuilder.newBuilder
            .append(
              render(List("Field Name", "Type", "Field Class Type"),
                     res.fields.map(x =>
                       List(x.name, x.`type`, x.fieldClassType.toString.replace("FieldType", "").toUpperCase)))
            )
            .append("\n")
            .append(res.metricInfo
              .map(info =>
                render(
                  List("Property", "Value"),
                  List(List("shardInterval", s"${info.shardInterval.milliseconds.toSeconds} seconds"),
                       List("retention", s"${info.retention.milliseconds.toSeconds} seconds"))
              ))
              .getOrElse(""))
            .toString()
        )
      case res: NamespacesListRetrieved =>
        Try(render(List("Namespace Name"), res.namespaces.map(name => List(name)).toList))
      case res: CommandStatementExecutedWithFailure =>
        Try(res.reason)
    }
  }

  /**
    * Renders results, building its [[String]] representation.
    * It makes use of [[AsciiTable]] to provide a table representation of input results
    *
    * @param headerColumns [[List]] containing headers name
    * @param rows Matrix representing data entries string values
    * @return [[String]] of table representation
    */
  private def render(headerColumns: List[FieldName], rows: List[Row]): String = {
    val at = new AsciiTable()
    // header
    at.addRule
    at.getRenderer.setCWC(new CWC_LongestWord())
    at.getContext.setWidth(tableMaxWidth)
    // prepending timestamp and value
    at.addRow(headerColumns: _*)

    // values
    for {
      row <- rows
      _ = at.addRule()
      _ = at.addRow(row: _*)
    } ()

    at.addRule()
    at.getContext.setGrid(A7_Grids.minusBarPlusEquals)

    at.render
  }
}
