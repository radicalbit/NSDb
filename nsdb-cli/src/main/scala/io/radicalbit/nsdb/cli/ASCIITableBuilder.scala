package io.radicalbit.nsdb.cli

import cats.implicits._
import de.vandermeer.asciitable.AsciiTable
import de.vandermeer.asciithemes.a7.A7_Grids
import io.radicalbit.nsdb.common.protocol.{MetricSchemaRetrieved, NamespaceMetricsListRetrieved, SQLStatementExecuted}

import scala.util.Try

object ASCIITableBuilder {

  private def extractColumnNames(stm: SQLStatementExecuted): Map[String, Option[String]] =
    stm.res
      .flatMap(_.dimensions.map {
        case (name, _) if (name.trim.length > 0) => (name.trim, None)
      })
      .toMap

  def tableFor(stm: SQLStatementExecuted): Try[String] =
    Try {
      val at                                         = new AsciiTable()
      val allDimensions: Map[String, Option[String]] = extractColumnNames(stm)

      val rows: List[List[String]] = stm.res.toList.map { x =>
        val dimensionsMap    = x.dimensions.map { case (k, v)                                     => (k, Some(v.toString)) }
        val mergedDimensions = allDimensions.combine(dimensionsMap).toList.sortBy { case (col, _) => col }
        // prepending timestamp and value
        x.timestamp.toString +: x.value.toString +: mergedDimensions.map { case (_, value) => value getOrElse ("") }
      }

      render("timestamp" +: "value" +: allDimensions.toList.map(_._1).sorted, rows)
    }

  def tableFor(command: NamespaceMetricsListRetrieved): Try[String] =
    Try(render(List("Metric Name"), List(command.metrics)))

  def tableFor(command: MetricSchemaRetrieved): Try[String] =
    Try(render(List("Field Name", "Type"), command.fields.map(x => List(x.name, x.`type`))))

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
