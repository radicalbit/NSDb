package io.radicalbit.nsdb
import java.io.BufferedReader

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.statement.{InsertSQLStatement, SelectSQLStatement}

import scala.concurrent.Await
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}

class NsdbILoop(in0: Option[BufferedReader], out: JPrintWriter) extends ILoop(in0, out) {

  def this(in: BufferedReader, out: JPrintWriter) = this(Some(in), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

  implicit lazy val system = ActorSystem("nsdb-cli", ConfigFactory.load("cli"), getClass.getClassLoader)

  val clientDelegate = new ClientDelegate()

  override def prompt = "nsdb $ "

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
    import scala.concurrent.duration._
    if (line startsWith ":") colonCommand(line)
    else if (intp.global == null) Result(keepRunning = false, None) // Notice failure to create compiler
    else if (new SQLStatementParser().parse(line).isSuccess) {
      val statement = new SQLStatementParser().parse(line).get
      val result = statement match {
        case s: SelectSQLStatement => Await.result(clientDelegate.executeSqlSelectStatement(s), 10 seconds)
        case s: InsertSQLStatement => Await.result(clientDelegate.executeSqlInsertStatement(s), 10 seconds)
      }
      Result(keepRunning = true, Some(result.toString))
    } else Result(keepRunning = true, interpretStartingWith(line))
  }

}
