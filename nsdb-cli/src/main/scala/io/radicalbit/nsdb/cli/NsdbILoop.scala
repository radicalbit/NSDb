package io.radicalbit.nsdb.cli

import java.io.BufferedReader

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.client.Client
import io.radicalbit.nsdb.cluster.endpoint.EndpointActor.SQLStatementExecuted
import io.radicalbit.nsdb.sql.parser.SQLStatementParser

import scala.concurrent.Await
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.util.{Failure, Success, Try}

class NsdbILoop(in0: Option[BufferedReader], out: JPrintWriter) extends ILoop(in0, out) {

  def this(in: BufferedReader, out: JPrintWriter) = this(Some(in), out)

  def this() = this(None, new JPrintWriter(Console.out, true))

  implicit lazy val system = ActorSystem("nsdb-cli", ConfigFactory.load("cli"), getClass.getClassLoader)

  val clientDelegate = new Client()

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

      val statement = new SQLStatementParser().parse(line)
      val result = statement.flatMap { stm =>
        Try(Await.result(clientDelegate.executeSqlStatement(stm), 10 seconds))
      }
      print(result)

      Result(keepRunning = true, result.map(_.res.mkString).toOption)
    } else Result(keepRunning = true, interpretStartingWith(line))
  }

  def print(result: Try[SQLStatementExecuted]): Unit = result match {
    case Success(stm) => echo(stm.res.mkString)
    case Failure(t)   => echo(s"Error while trying to run the SQL statement. The detailed error is ${t.getMessage}")
  }
}
