package io.radicalbit.nsdb.cli.console

import java.io.BufferedReader

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.cli.table.ASCIITableBuilder
import io.radicalbit.nsdb.client.akka.AkkaClusterClient
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.sql.parser.{CommandStatementParser, SQLStatementParser}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.util.{Failure, Success, Try}

class NsdbILoop(host: Option[String], port: Option[Int], db: String, in0: Option[BufferedReader], out: JPrintWriter)
    extends ILoop(in0, out)
    with LazyLogging {

  def this(in: BufferedReader, out: JPrintWriter) = this(None, None, "root", Some(in), out)

  def this(host: Option[String], port: Option[Int], db: String) =
    this(host, port, db, None, new JPrintWriter(Console.out, true))

  implicit lazy val system: ActorSystem = ActorSystem("nsdb-cli", ConfigFactory.load("cli"), getClass.getClassLoader)

  val clientDelegate = new AkkaClusterClient(host getOrElse "127.0.0.1", port getOrElse 2552)

  val commandStatementParser = new CommandStatementParser(db)

  val sqlStatementParser = new SQLStatementParser()

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
    else if (intp.global == null) Result(keepRunning = false, None) // Notice failure to create compiler
    else
      parseStatement(line).getOrElse {
        echo("Cannot parse the inserted statement.")
        result()
      }
  }

  def result(lineToRecord: Option[String] = None) = Result(keepRunning = true, lineToRecord = lineToRecord)

  def parseStatement(statement: String): Try[Result] =
    parseCommandStatement(statement).orElse(parseSQLStatement(statement))

  def parseCommandStatement(statement: String): Try[Result] =
    commandStatementParser.parse(currentNamespace, statement).map(x => sendCommand(x, statement))

  def parseSQLStatement(statement: String): Try[Result] =
    currentNamespace
      .map { namespace =>
        sqlStatementParser.parse(db, namespace, statement).map(x => sendCommand(x, statement))
      }
      .getOrElse(Failure(new RuntimeException("Select a namespace before tu run non-namespace related statements.")))

  def sendCommand(stm: NSDBStatement, lineToRecord: String): Result = stm match {
    case ShowNamespaces =>
      echo("Work in progress...")
      result()
    case UseNamespace(namespace) =>
      currentNamespace = Some(namespace)
      echo(s"The namespace $namespace has been selected.")
      result()
    case x: ShowMetrics =>
      send(clientDelegate.executeCommandStatement(x), ASCIITableBuilder.tableForMetrics, lineToRecord)
    case x: DescribeMetric =>
      send(clientDelegate.executeCommandStatement(x), ASCIITableBuilder.tableForDescribeMetric, lineToRecord)
    case x: SQLStatement =>
      send(clientDelegate.executeSqlStatement(x), ASCIITableBuilder.tableFor, lineToRecord)
  }

  def send[T <: EndpointOutputProtocol](send: => Future[EndpointOutputProtocol],
                                        print: T => Try[String],
                                        lineToRecord: String): Result =
    Try(Await.result(send, 10 seconds)) match {
      case Success(resp: T) => echo(print(resp), lineToRecord)
      case Success(resp: SQLStatementFailed) =>
        echo(s"statement failed because ${resp.reason}")
        result(Some(lineToRecord))
      case Success(_) =>
        echo(
          "The NSDB cluster did not fulfill the request successfully. Please check the connection or run a lightweight query.")
        result(Some(lineToRecord))
      case Failure(ex) =>
        logger.error("error", ex)
        echo(
          "The NSDB cluster did not fulfill the request successfully. Please check the connection or run a lightweight query.")
        result(Some(lineToRecord))
    }

  def echo(out: Try[String], lineToRecord: String): Result = out match {
    case Success(x) =>
      echo("\n")
      echo(x)
      echo("\n")
      result(Some(lineToRecord))
    case Failure(t) =>
      echo("Cannot show the result in a visual way.")
      result(Some(lineToRecord))
  }
}
