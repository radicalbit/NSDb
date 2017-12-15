package io.radicalbit.nsdb.cli.table

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import de.vandermeer.asciitable.AsciiTable
import de.vandermeer.asciithemes.a7.A7_Grids
import io.radicalbit.nsdb.common.protocol._

import scala.util.Try

object ASCIITableBuilder extends LazyLogging {

  private def extractColumnNames(stm: SQLStatementExecuted): Map[String, Option[String]] =
    stm.res
      .flatMap(_.dimensions.map {
        case (name, _) if (name.trim.length > 0) => (name.trim, None)
      })
      .toMap

  def tableFor(stm: SQLStatementResult): Try[String] =
    stm match {
      case statement: SQLStatementExecuted =>
        Try {
          val at                                         = new AsciiTable()
          val allDimensions: Map[String, Option[String]] = extractColumnNames(statement)

          val rows: List[List[String]] = statement.res.toList.map { x =>
            val dimensionsMap    = x.dimensions.map { case (k, v)                                     => (k, Some(v.toString)) }
            val mergedDimensions = allDimensions.combine(dimensionsMap).toList.sortBy { case (col, _) => col }
            // prepending timestamp and value
            x.timestamp.toString +: x.value.toString +: mergedDimensions.map {
              case (_, value) => value getOrElse ("")
            }
          }

          render("timestamp" +: "value" +: allDimensions.toList.map(_._1).sorted, rows)
        }
      case failStatement: SQLStatementFailed =>
        Try(failStatement.reason)

    }

  def tableFor(commandResult: CommandStatementExecuted): Try[String] = {
    commandResult match {
      case res: NamespaceMetricsListRetrieved =>
        Try(render(List("Metric Name"), List(res.metrics)))
      case res: MetricSchemaRetrieved =>
        Try(render(List("Field Name", "Type"), res.fields.map(x => List(x.name, x.`type`))))
      case res: NamespacesListRetrieved =>
        logger.info("Namespaces: {}", res.namespaces)
        Try(render(List("Name"), res.namespaces.map(name => List(name)).toList))
      case res: CommandStatementExecutedWithFailure =>
        Try(res.reason)
    }
  }

  private def render(headerColumns: List[String], rows: List[List[String]]): String = {
    val at = new AsciiTable()
    // header
    at.addRule
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
