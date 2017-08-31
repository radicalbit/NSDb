package io.radicalbit.nsdb.cli

import java.io.BufferedReader

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.client.akka.AkkaClusterClient
import io.radicalbit.nsdb.common.protocol.SQLStatementExecuted
import io.radicalbit.nsdb.sql.parser.SQLStatementParser

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.util.{Failure, Success, Try}

class NsdbILoop(in0: Option[BufferedReader], out: JPrintWriter) extends ILoop(in0, out) {

  def this(in: BufferedReader, out: JPrintWriter) = this(Some(in), out)

  def this() = this(None, new JPrintWriter(Console.out, true))

  implicit lazy val system = ActorSystem("nsdb-cli", ConfigFactory.load("cli"), getClass.getClassLoader)

  val clientDelegate = new AkkaClusterClient()

  val parser = new SQLStatementParser()

  var currentNamespace: Option[String] = None

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
    if (line startsWith ":") colonCommand(line)
    // FIXME: this is only a workaround to select a namespace, please move the CLI commands inside a dedicated parser
    else if (line startsWith "use ") {
      currentNamespace = Some(line.split("use")(1).trim)
      currentNamespace.foreach(x => echo(s"Namespace '$x' has been selected."))
      Result(keepRunning = true, lineToRecord = None)
    } else if (intp.global == null) Result(keepRunning = false, None) // Notice failure to create compiler
    else parseAndExecute(line)

  }

  def parseAndExecute(statement: String): Result =
    currentNamespace.map { namespace =>
      parser.parse(namespace, statement).map { stm =>
        val result = Try(Await.result(clientDelegate.executeSqlStatement(stm), 10 seconds))
        print(result)
        Result(keepRunning = true, result.map(_.res.mkString).toOption)
      } getOrElse {
        echo("The inserted SQL statement is invalid.")
        Result(keepRunning = true, lineToRecord = None)
      }
    } getOrElse {
      echo("Please select a valid namespace before the execution of any SQL statement.")
      Result(keepRunning = true, lineToRecord = None)
    }

  def print(result: Try[SQLStatementExecuted]): Unit = result match {
    case Success(stm) =>
      echo(stm.res.mkString)
    case Failure(t) =>
      echo(s"Error while trying to run the inserted SQL statement. The detailed error is ${t.getMessage}")
  }
}
