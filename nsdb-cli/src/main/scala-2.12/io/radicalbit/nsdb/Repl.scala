package io.radicalbit.nsdb
import java.io.BufferedReader

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.tools.nsc.util.stringFromStream

class NsdbILoop(in0: Option[BufferedReader], out: JPrintWriter) extends ILoop(in0, out) {

  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

  override def prompt = "nsdb $ "

//  addThunk {
//    intp.beQuietDuring {
//      intp.addImports("java.lang.Math._")
//    }
//  }

  override def printWelcome() {
    echo("""
        | _______             .______.
        | \      \   ______ __| _/\_ |__
        | /   |   \ /  ___// __ |  | __ \
        |/    |    \\___ \/ /_/ |  | \_\ \
        |\____|__  /____  >____ |  |___  /
        |        \/     \/     \/      \/
        |
      """.stripMargin)
  }

  override def command(line: String): Result = {
    super.command(line)
  }

}

object NsdbILoop {

  /**
    * Creates an interpreter loop with default settings and feeds
    * the given code to it as input.
    */
  def run(code: String, sets: Settings = new Settings): String = {
    import java.io.{BufferedReader, StringReader, OutputStreamWriter}

    stringFromStream { ostream =>
      println("OSTREAM")
      println(ostream)
      Console.withOut(ostream) {
        val input  = new BufferedReader(new StringReader(code))
        val output = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl   = new NsdbILoop(input, output)

        if (sets.classpath.isDefault) {
          sets.classpath.value = sys.props("java.class.path")
        }
        repl process sets
      }
    }
  }
  def run(lines: List[String]): String = run(lines.map(_ + "\n").mkString)
}
