package io.radicalbit.nsdb.cli.console

import java.io.BufferedReader

import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.cli.table.ASCIITableBuilder
import io.radicalbit.nsdb.client.rpc.GRPCClient
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.rpc.requestCommand.{
  DescribeMetric => GrpcDescribeMetric,
  ShowMetrics => GrpcShowMetrics,
  ShowNamespaces => GrpcShowNamespaces
}
import io.radicalbit.nsdb.rpc.responseCommand.{
  Namespaces,
  MetricSchemaRetrieved => GrpcMetricSchemaRetrieved,
  MetricsGot => GrpcMetricsGot
}
import io.radicalbit.nsdb.common.protocol.{CommandStatementExecuted, _}
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse
import io.radicalbit.nsdb.sql.parser.CommandStatementParser

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class NsdbILoop(host: Option[String], port: Option[Int], db: String, in0: Option[BufferedReader], out: JPrintWriter)
    extends ILoop(in0, out)
    with LazyLogging {

  def this(in: BufferedReader, out: JPrintWriter) = this(None, None, "root", Some(in), out)

  def this(host: Option[String], port: Option[Int], db: String) =
    this(host, port, db, None, new JPrintWriter(Console.out, true))

  val commandStatementParser = new CommandStatementParser(db)

  val clientGrpc = new GRPCClient(host = host.getOrElse("127.0.0.1"), port = port.getOrElse(7817))

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
      //FIXME: Match possible failures
      parseStatement(line).getOrElse {
        echo("Cannot parse the inserted statement.")
        result()
      }
  }

  def result(lineToRecord: Option[String] = None) = Result(keepRunning = true, lineToRecord = lineToRecord)

  def parseStatement(statement: String): Try[Result] =
    sendParsedCommandStatement(statement)
      .orElse(
        Try(
          processSQLStatementResponse(prepareSQLStatement(statement).map(toInternalSQLStatementResponse),
                                      ASCIITableBuilder.tableFor,
                                      statement)
        ))

  def sendParsedCommandStatement(statement: String): Try[Result] =
    commandStatementParser.parse(currentNamespace, statement).map(x => sendCommand(x, statement))

  def prepareSQLStatement(statement: String): Future[SQLStatementResponse] =
    currentNamespace match {
      case Some(namespace) => clientGrpc.executeSQLStatement(SQLRequestStatement(db, namespace, statement))
      case None =>
        Future.successful(
          SQLStatementResponse(
            db = db,
            completedSuccessfully = false,
            reason = "Namespace must be selected",
            message = "Namespace must be selected"
          ))
    }

  def toInternalCommandResponse[T](gRpcResponse: T): CommandStatementExecuted =
    gRpcResponse match {
      case r: GrpcMetricsGot if r.completedSuccessfully =>
        NamespaceMetricsListRetrieved(r.db, r.namespace, r.metrics.toList)
      case r: GrpcMetricSchemaRetrieved if r.completedSuccessfully =>
        MetricSchemaRetrieved(r.db,
                              r.namespace,
                              r.metric,
                              r.fields.map(field => MetricField(field.name, field.`type`)).toList)
      case r: Namespaces if r.completedSuccessfully =>
        NamespacesListRetrieved(r.db, r.namespaces)
      case r: Namespaces =>
        CommandStatementExecutedWithFailure(r.errors)
      case r: GrpcMetricsGot =>
        CommandStatementExecutedWithFailure(r.errors)
      case r: GrpcMetricSchemaRetrieved =>
        CommandStatementExecutedWithFailure(r.errors)
    }

  def toInternalSQLStatementResponse(gRpcResponse: SQLStatementResponse): SQLStatementResult = {
    if (gRpcResponse.completedSuccessfully) {
      SQLStatementExecuted(
        db = gRpcResponse.db,
        namespace = gRpcResponse.namespace,
        metric = gRpcResponse.metric,
        res = gRpcResponse.records.map(
          r =>
            Bit(
              r.timestamp,
              r.value.value.asInstanceOf[JSerializable],
              r.dimensions.map {
                case (k, dim) =>
                  (k, dim.value.value.asInstanceOf[JSerializable])
              }
          ))
      )
    } else {
      SQLStatementFailed(
        db = gRpcResponse.db,
        namespace = gRpcResponse.namespace,
        metric = gRpcResponse.metric,
        reason = gRpcResponse.reason
      )
    }
  }

  def sendCommand(stm: CommandStatement, lineToRecord: String): Result = stm match {
    case ShowNamespaces =>
      processCommandResponse[CommandStatementExecuted](
        clientGrpc
          .showNamespaces(GrpcShowNamespaces(db))
          .map(toInternalCommandResponse[Namespaces]),
        ASCIITableBuilder.tableFor,
        lineToRecord
      )
      result()
    case UseNamespace(namespace) =>
      currentNamespace = Some(namespace)
      echo(s"The namespace $namespace has been selected.")
      result()
    case ShowMetrics(_, namespace) =>
      processCommandResponse[CommandStatementExecuted](
        clientGrpc
          .showMetrics(GrpcShowMetrics(db, namespace))
          .map(toInternalCommandResponse[GrpcMetricsGot]),
        ASCIITableBuilder.tableFor,
        lineToRecord
      )
    case DescribeMetric(_, namespace, metric) =>
      processCommandResponse[CommandStatementExecuted](
        clientGrpc
          .describeMetrics(GrpcDescribeMetric(db, namespace, metric))
          .map(toInternalCommandResponse[GrpcMetricSchemaRetrieved]),
        ASCIITableBuilder.tableFor,
        lineToRecord
      )
  }

  def processCommandResponse[T <: CommandStatementExecuted](attemptValue: Future[T],
                                                            print: T => Try[String],
                                                            lineToRecord: String): Result =
    Try(Await.result(attemptValue, 10 seconds)) match {
      case Success(resp: CommandStatementExecutedWithFailure) =>
        echo(s"Statement failed because ${resp.reason}")
        result(Some(lineToRecord))
      case Success(resp) =>
        echo(print(resp), lineToRecord)
      case Failure(ex) =>
        logger.error("error", ex)
        echo(
          "The NSDB cluster did not fulfill the request successfully. Please check the connection or run a lightweight query.")
        result(Some(lineToRecord))
    }

  def processSQLStatementResponse(statementAttempt: Future[SQLStatementResult],
                                  print: SQLStatementResult => Try[String],
                                  lineToRecord: String): Result = {
    Try(Await.result(statementAttempt, 10 seconds)) match {
      case Success(resp: SQLStatementFailed) =>
        echo(s"statement failed because ${resp.reason}")
        result(Some(lineToRecord))
      case Success(resp: SQLStatementExecuted) =>
        echo(print(resp), lineToRecord)
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
