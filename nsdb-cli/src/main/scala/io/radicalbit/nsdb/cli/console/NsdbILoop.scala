/*
 * Copyright 2018-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.cli.console

import java.io.BufferedReader

import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.cli.table.ASCIITableBuilder
import io.radicalbit.nsdb.client.rpc.GRPCClient
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.rpc.health.HealthCheckResponse
import io.radicalbit.nsdb.rpc.health.HealthCheckResponse.ServingStatus
import io.radicalbit.nsdb.rpc.requestCommand.{
  DescribeMetric => GrpcDescribeMetric,
  ShowMetrics => GrpcShowMetrics,
  ShowNamespaces => GrpcShowNamespaces
}
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.responseCommand.{
  Namespaces,
  DescribeMetricResponse => GrpcDescribeMetricResponse,
  MetricsGot => GrpcMetricsGot
}
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import io.radicalbit.nsdb.sql.parser._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.util.{Failure, Success, Try}

/**
  * Nsdb Command Line Interface main class extending scala standard REPL [[scala.tools.nsc.interpreter.ILoop]].
  * Internally, this class manages a gRPC Client of [[io.radicalbit.nsdb.client.rpc.GRPCClient]] class used to send queries and command statements to Nsdb cluster.
  * User input is parsed using two methodologies: command statements are parsed client side using [[CommandStatementParser]],
  * whereas sql statements are interpreted on cluster side.
  * The database information, to establish connection with, must be specified in class constructor,
  * otherwise default parameters values are used.
  * Once created, user must define database namespace on which run statements.
  *
  * @param host Nsdb cluster grpc server ip address
  * @param port Nsdb cluster grpc server port
  * @param db Nsdb database name to establish connection with
  * @param in0 [[BufferedReader]] used to acquire user input
  * @param out REPL printer for commands response
  */
class NsdbILoop(host: Option[String],
                port: Option[Int],
                db: String,
                tableMaxWidth: Option[Int],
                in0: Option[BufferedReader],
                out: JPrintWriter)
    extends ILoop(in0, out)
    with LazyLogging {

  def this(in: BufferedReader, out: JPrintWriter) = this(None, None, "root", None, Some(in), out)

  def this(host: Option[String], port: Option[Int], db: String, tableMaxWidth: Option[Int]) = {
    this(host, port, db, tableMaxWidth, None, new JPrintWriter(Console.out, true))
    val instance = s"${host.getOrElse("127.0.0.1")} : ${port.getOrElse(7817)}"
    Await
      .ready(clientGrpc.checkConnection(), 10.seconds)
      .value
      .getOrElse(Success(HealthCheckResponse(ServingStatus.UNKNOWN))) match {
      case Success(response) if response.status.isServing => //do nothing
      case Success(_) =>
        sys.error(s"instance $instance is not available at the moment.")
      case Failure(ex) =>
        logger.error("", ex)
        sys.error(s"error while connecting to instance $instance : ${ex.getMessage}")
    }
  }

  val commandStatementParser = new CommandStatementParser(db)

  val clientGrpc = new GRPCClient(host = host.getOrElse("127.0.0.1"), port = port.getOrElse(7817))

  var currentNamespace: Option[String] = None

  val tableBuilder = new ASCIITableBuilder(tableMaxWidth = tableMaxWidth.getOrElse(100))

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

  /**
    * Tries to parse user input as a [[CommandStatement]] otherwise treats it as an [[SQLRequestStatement]] sent to the server.
    *
    * @param statement string representing user input
    * @return [[Result]] to be printed on REPL
    */
  def parseStatement(statement: String): Try[Result] =
    sendParsedCommandStatement(statement)
      .orElse(
        Try(
          processSQLStatementResponse(prepareSQLStatement(statement).map(toInternalSQLStatementResponse),
                                      tableBuilder.tableFor,
                                      statement)
        ))

  /**
    * Parses User command statement String into a [[CommandStatement]] and sends it to server
    *
    * @param statement user input
    * @return [[Result]] to be printed on REPL
    */
  def sendParsedCommandStatement(statement: String): Try[Result] =
    commandStatementParser.parse(currentNamespace, statement) match {
      case CommandStatementParserSuccess(_, parsedStatement) => Try(sendCommand(parsedStatement, statement))
      case CommandStatementParserFailure(_, error)           => Failure(new RuntimeException(error))
    }

  /**
    * If working namespace is defined, sends an async request containing the unparsed query statement to Nsdb server otherwise
    * return a failure described in [[SQLStatementResponse]]
    *
    * @param statement [[String]] statement
    * @return
    */
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

  private def fieldClassTypeFor(field: GrpcDescribeMetricResponse.MetricField): FieldClassType = {
    import GrpcDescribeMetricResponse.MetricField.FieldClassType._

    field.fieldClassType match {
      case DIMENSION => DimensionFieldType
      case TAG       => TagFieldType
      case TIMESTAMP => TimestampFieldType
      case VALUE     => ValueFieldType
    }
  }

  /**
    * Manages server responses from [[CommandStatement]] client requests
    * Transform gRPC protocol depending messages into client managed ones.
    *
    * @param gRpcResponse server response oneOf [[GrpcMetricsGot]], [[GrpcDescribeMetricResponse]],
    * @tparam T type parameter of gRpcResponse
    * @return internal representation on gRPC response
    */
  private def toInternalCommandResponse[T](gRpcResponse: T): CommandStatementExecuted =
    gRpcResponse match {
      case r: GrpcMetricsGot if r.completedSuccessfully =>
        NamespaceMetricsListRetrieved(r.db, r.namespace, r.metrics.toList)
      case r: GrpcDescribeMetricResponse if r.completedSuccessfully =>
        DescribeMetricResponse(
          r.db,
          r.namespace,
          r.metric,
          r.fields.map(field => MetricField(field.name, fieldClassTypeFor(field), field.indexType)).toList,
          metricInfo = r.metricInfo.map(mi => MetricInfo(r.db, r.namespace, r.metric, mi.shardInterval, mi.retention))
        )
      case r: Namespaces if r.completedSuccessfully =>
        NamespacesListRetrieved(r.db, r.namespaces)
      case r: Namespaces =>
        CommandStatementExecutedWithFailure(r.errors)
      case r: GrpcMetricsGot =>
        CommandStatementExecutedWithFailure(r.errors)
      case r: GrpcDescribeMetricResponse =>
        CommandStatementExecutedWithFailure(r.errors)
    }

  /**
    * Manages server responses from [[SQLRequestStatement]] client requests
    * Transform gRPC protocol depending messages into client managed ones.
    *
    * @param gRpcResponse server response as [[SQLStatementResponse]]
    * @return a [[SQLStatementResult]] that can be a [[SQLStatementExecuted]] or a [[SQLStatementFailed]]
    */
  private def toInternalSQLStatementResponse(gRpcResponse: SQLStatementResponse): SQLStatementResult = {
    if (gRpcResponse.completedSuccessfully) {
      SQLStatementExecuted(
        db = gRpcResponse.db,
        namespace = gRpcResponse.namespace,
        metric = gRpcResponse.metric,
        res = gRpcResponse.records.map(
          r =>
            Bit(
              r.timestamp,
              NSDbNumericType(r.value.value),
              r.dimensions.map {
                case (k, dim) =>
                  (k, NSDbType(dim.value.value))
              },
              r.tags.map {
                case (k, dim) =>
                  (k, NSDbType(dim.value.value))
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

  /**
    * Sends a [[CommandStatement]] to the server and handles its response.
    *
    * @param stm [[CommandStatement]] to be sent
    * @param lineToRecord line to be displayed
    * @return
    */
  private def sendCommand(stm: CommandStatement, lineToRecord: String): Result = stm match {
    case ShowNamespaces =>
      processCommandResponse[CommandStatementExecuted](
        clientGrpc
          .showNamespaces(GrpcShowNamespaces(db))
          .map(toInternalCommandResponse[Namespaces]),
        tableBuilder.tableFor,
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
        tableBuilder.tableFor,
        lineToRecord
      )
    case DescribeMetric(_, namespace, metric) =>
      processCommandResponse[CommandStatementExecuted](
        clientGrpc
          .describeMetric(GrpcDescribeMetric(db, namespace, metric))
          .map(toInternalCommandResponse[GrpcDescribeMetricResponse]),
        tableBuilder.tableFor,
        lineToRecord
      )
  }

  private def processCommandResponse[T <: CommandStatementExecuted](attemptValue: Future[T],
                                                                    print: T => Try[String],
                                                                    lineToRecord: String): Result =
    Try(Await.result(attemptValue, 10 seconds)) match {
      case Success(resp: CommandStatementExecutedWithFailure) =>
        echo(s"Statement failed because : ${resp.reason}")
        result(Some(lineToRecord))
      case Success(resp) =>
        echo(print(resp), lineToRecord)
      case Failure(ex) =>
        logger.error("error", ex)
        echo(
          "The NSDB cluster did not fulfill the request successfully. Please check the connection or run a lightweight query.")
        result(Some(lineToRecord))
    }

  private def processSQLStatementResponse(statementAttempt: Future[SQLStatementResult],
                                          print: SQLStatementResult => Try[String],
                                          lineToRecord: String): Result = {
    val initialTimestamp = System.currentTimeMillis()
    Try(Await.result(statementAttempt, 30 seconds)) match {
      case Success(resp: SQLStatementFailed) =>
        echo(s"Statement failed because : ${resp.reason}")
        result(Some(lineToRecord))
      case Success(resp: SQLStatementExecuted) =>
        echo(
          print(resp),
          lineToRecord,
          Some(
            FiniteDuration(System.currentTimeMillis() - initialTimestamp, java.util.concurrent.TimeUnit.MILLISECONDS)))
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

  private def echo(out: Try[String], lineToRecord: String, duration: Option[Duration] = None): Result = out match {
    case Success(x) =>
      echo("\n")
      echo(x)
      echo("\n")
      duration.foreach { d =>
        echo("\n")
        echo("\n")
        echo(s"Executed in ${d.toSeconds.toString} seconds")
      }
      result(Some(lineToRecord))
    case Failure(t) =>
      logger.error("Error in render", t)
      echo("Cannot show the result in a visual way.")
      result(Some(lineToRecord))
  }
}
