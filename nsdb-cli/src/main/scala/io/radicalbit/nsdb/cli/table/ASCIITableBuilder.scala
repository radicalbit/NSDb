package io.radicalbit.nsdb.cli.table

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import de.vandermeer.asciitable.{AsciiTable, CWC_LongestWord}
import de.vandermeer.asciithemes.a7.A7_Grids
import io.radicalbit.nsdb.common.protocol._

import scala.util.Try

/**
  * Object used to render server responses on scala REPL.
  * It provides a table representation of query and command results making use of [[AsciiTable]].
  *
  */
object ASCIITableBuilder extends LazyLogging {

  type DimensionName = String
  type Row           = List[String]

  /**
    * Defines table maxWidth, if the number of char of a single line is higher than this value,
    * just a fixed number of dimension is rendered.
    */
  private val tableMaxWidth = 100

  /**
    * Defines the number of dimension rendered in case of table exceeding tableMaxWidth
    * It doesn't take count fo timestamp and value fixed columns
    */
  private val dimensionLimit = 3

  /**
    * Extract ColumnNames from a successful [[SQLStatementExecuted]]
    * @param stm sql statement response
    * @return [[Map]] representation of [[Bit]] dimensions
    */
  private def extractColumnNames(stm: SQLStatementExecuted): Map[DimensionName, Option[String]] =
    stm.res
      .flatMap(_.dimensions.map {
        case (name, _) if (name.trim.length > 0) => (name.trim, None)
      })
      .toMap

  def tableFor(stm: SQLStatementResult): Try[String] =
    stm match {
      case statement: SQLStatementExecuted if statement.res.nonEmpty =>
        Try {
          val at                                                = new AsciiTable()
          val allDimensions: Map[DimensionName, Option[String]] = extractColumnNames(statement)

          val rows: List[Row] = statement.res.toList.map { x =>
            val dimensionsMap    = x.dimensions.map { case (k, v) => (k, Some(v.toString)) }
            val mergedDimensions = allDimensions.combine(dimensionsMap).toList
            // prepending timestamp and value
            x.timestamp.toString +: x.value.toString +: mergedDimensions.map {
              case (_, value) => value getOrElse ("")
            }
          }

          val maxDimensionNumber = allDimensions.keys.size

          // Find max num of char for a file, if so omit some dimensions from rendering
          if (Math.max(allDimensions.keys.foldLeft(0)((acc, k) => acc + k.length),
                       allDimensions.values.flatten.foldLeft(0)((acc, k) => acc + k.length)) < tableMaxWidth)
            render("timestamp" +: "value" +: allDimensions.toList.map(_._1).sorted, rows)
          else {
            // Add a new column containing as header the number of omitted dimensions
            val nMoreDimensions = maxDimensionNumber - dimensionLimit
            val summaryHeader   = List(s"$nMoreDimensions more dimensions")
            val headers = List("timestamp", "value") ++ allDimensions.toList
              .map(_._1)
              .sorted
              .take(dimensionLimit) ++ summaryHeader
            // take 2 more columns for timestamp and value, add one more columns for summary of remaining dimensions
            val newRows = rows.map(column => column.take(dimensionLimit + 2) ++ List(""))
            render(headers, newRows)
          }

        }

      case statement: SQLStatementExecuted if statement.res.isEmpty =>
        Try("Statement executed successfully, no records to display")

      case failStatement: SQLStatementFailed =>
        Try(failStatement.reason)

    }

  def tableFor(commandResult: CommandStatementExecuted): Try[String] = {
    commandResult match {
      case res: NamespaceMetricsListRetrieved =>
        Try(render(List("Metric Name"), res.metrics.map(m => List(m))))
      case res: MetricSchemaRetrieved =>
        Try(render(List("Field Name", "Type"), res.fields.map(x => List(x.name, x.`type`))))
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
  private def render(headerColumns: List[DimensionName], rows: List[Row]): String = {
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
