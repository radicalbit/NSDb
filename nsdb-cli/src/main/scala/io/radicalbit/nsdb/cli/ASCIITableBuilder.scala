package io.radicalbit.nsdb.cli

import de.vandermeer.asciitable.AsciiTable
import de.vandermeer.asciithemes.a7.A7_Grids
import io.radicalbit.nsdb.common.protocol.SQLStatementExecuted

import scala.util.{Failure, Try}

object ASCIITableBuilder {

  def tableFor(stm: SQLStatementExecuted): Try[String] =
    Try {

      val at = new AsciiTable()

//    stm.res.map(b => b.)

      // header
      at.addRule

      for {
        bit <- stm.res
        _ = at.addRule()
        _ = at.addRow(bit.timestamp.toString)
      } ()

      at.addRule()

      at.getContext.setGrid(A7_Grids.minusBarPlusEquals)

      at.render
    }.recoverWith {
      case t =>
        t.printStackTrace()
        Failure(t)
    }
}
