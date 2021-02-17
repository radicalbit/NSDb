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

package io.radicalbit.nsdb.rpc.server.endpoint

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.client.rpc.converter.GrpcBitConverters._
import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{
  DeleteSQLStatement,
  DropSQLStatement,
  InsertSQLStatement,
  SelectSQLStatement
}
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{
  DropMetric,
  ExecuteDeleteStatement,
  ExecuteStatement,
  MapInput
}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.rpc.common.{Dimension, Tag}
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse
import io.radicalbit.nsdb.rpc.server.GRPCService
import io.radicalbit.nsdb.rpc.service.NSDBServiceSQLGrpc.NSDBServiceSQL
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.sql.parser.StatementParserResult.{SqlStatementParserFailure, SqlStatementParserSuccess}
import org.slf4j.LoggerFactory
import scalapb.descriptors.ServiceDescriptor

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Concrete implementation of the Sql Grpc service
  */
class GrpcEndpointServiceSQL(writeCoordinator: ActorRef, readCoordinator: ActorRef, parserSQL: SQLStatementParser)(
    implicit timeout: Timeout,
    executionContext: ExecutionContext)
    extends NSDBServiceSQL
    with GRPCService {

  private val log = LoggerFactory.getLogger(classOf[GrpcEndpointServiceSQL])

  override def serviceDescriptor: ServiceDescriptor = this.serviceCompanion.scalaDescriptor

  override def insertBit(request: RPCInsert): Future[RPCInsertResult] = {
    log.debug("Received a write request {}", request)

    val bit = Bit(
      timestamp = request.timestamp,
      dimensions = request.dimensions.collect {
        case (k, v) => (k, dimensionFor(v.value))
      },
      tags = request.tags.collect {
        case (k, v) => (k, tagFor(v.value))
      },
      value = valueFor(request.value)
    )

    val res = (writeCoordinator ? MapInput(
      db = request.database,
      namespace = request.namespace,
      metric = request.metric,
      ts = request.timestamp,
      record = bit
    )).map {
      case _: InputMapped =>
        RPCInsertResult(completedSuccessfully = true)
      case msg: RecordRejected =>
        RPCInsertResult(completedSuccessfully = false, errors = msg.reasons.mkString(","))
      case _ => RPCInsertResult(completedSuccessfully = false, errors = "unknown reason")
    } recover {
      case t =>
        log.error(s"error while inserting $bit", t)
        RPCInsertResult(completedSuccessfully = false, t.getMessage)
    }

    res.onComplete {
      case Success(res: RPCInsertResult) =>
        log.debug("Completed the write request {}", request)
        if (res.completedSuccessfully)
          log.debug("The result is {}", res)
        else
          log.error("The result is {}", res)
      case Failure(t: Throwable) =>
        log.error(s"error on request $request", t)
    }
    res
  }

  private def valueFor(v: RPCInsert.Value): NSDbNumericType = v match {
    case _: RPCInsert.Value.DecimalValue => NSDbNumericType(v.decimalValue.get)
    case _: RPCInsert.Value.LongValue    => NSDbNumericType(v.longValue.get)
  }

  private def dimensionFor(v: Dimension.Value): NSDbType = v match {
    case _: Dimension.Value.DecimalValue => NSDbType(v.decimalValue.get)
    case _: Dimension.Value.LongValue    => NSDbType(v.longValue.get)
    case _                               => NSDbType(v.stringValue.get)
  }

  private def tagFor(v: Tag.Value): NSDbType = v match {
    case _: Tag.Value.DecimalValue => NSDbType(v.decimalValue.get)
    case _: Tag.Value.LongValue    => NSDbType(v.longValue.get)
    case _                         => NSDbType(v.stringValue.get)
  }

  override def executeSQLStatement(
      request: SQLRequestStatement
  ): Future[SQLStatementResponse] = {
    val requestDb        = request.db
    val requestNamespace = request.namespace
    parserSQL.parse(request.db, request.namespace, request.statement) match {
      // Parsing Success
      case SqlStatementParserSuccess(_, statement) =>
        statement match {
          case select: SelectSQLStatement =>
            log.debug("Received a select request {}", select)
            (readCoordinator ? ExecuteStatement(select))
              .map {
                // SelectExecution Success
                case SelectStatementExecuted(statement, values: Seq[Bit], _) =>
                  log.debug("SQL statement succeeded on db {} with namespace {} and metric {}",
                            statement.db,
                            statement.namespace,
                            statement.metric)
                  SQLStatementResponse(
                    db = statement.db,
                    namespace = statement.namespace,
                    metric = statement.metric,
                    completedSuccessfully = true,
                    records = values.map(bit => bit.asGrpcBit)
                  )
                // SelectExecution Failure
                case SelectStatementFailed(statement, reason, _) =>
                  SQLStatementResponse(
                    db = requestDb,
                    namespace = requestNamespace,
                    completedSuccessfully = false,
                    reason = reason
                  )
              }
              .recoverWith {
                case t =>
                  log.error(s"Error in executing statement $statement", t)
                  Future.successful(
                    SQLStatementResponse(
                      db = requestDb,
                      namespace = requestNamespace,
                      completedSuccessfully = false,
                      reason = t.getMessage
                    ))
              }
          case insert: InsertSQLStatement =>
            log.debug("Received a insert request {}", insert)
            val result = InsertSQLStatement
              .unapply(insert)
              .map {
                case (db, namespace, metric, ts, dimensions, tags, value) =>
                  val timestamp = ts getOrElse System.currentTimeMillis
                  writeCoordinator ? MapInput(
                    timestamp,
                    db,
                    namespace,
                    metric,
                    Bit(timestamp = timestamp,
                        value = value,
                        dimensions = dimensions.map(_.fields).getOrElse(Map.empty),
                        tags = tags.map(_.fields).getOrElse(Map.empty))
                  )
              }
              .getOrElse(Future(throw new InvalidStatementException("The insert SQL statement is invalid.")))

            result
              .map {
                case InputMapped(db, namespace, metric, record) =>
                  SQLStatementResponse(db = db,
                                       namespace = namespace,
                                       metric = metric,
                                       completedSuccessfully = true,
                                       records = Seq(record.asGrpcBit))
                case msg: RecordRejected =>
                  SQLStatementResponse(db = msg.db,
                                       namespace = msg.namespace,
                                       metric = msg.metric,
                                       completedSuccessfully = false,
                                       reason = msg.reasons.mkString(","))
                case _ =>
                  SQLStatementResponse(db = insert.db,
                                       namespace = insert.namespace,
                                       metric = insert.metric,
                                       completedSuccessfully = false,
                                       reason = "unknown reason")
              }

          case delete: DeleteSQLStatement =>
            (writeCoordinator ? ExecuteDeleteStatement(delete))
              .mapTo[DeleteStatementExecuted]
              .map(
                x =>
                  SQLStatementResponse(db = x.db,
                                       namespace = x.namespace,
                                       metric = x.metric,
                                       completedSuccessfully = true,
                                       records = Seq.empty))
              .recoverWith {
                case t =>
                  Future.successful(
                    SQLStatementResponse(
                      db = requestDb,
                      namespace = requestNamespace,
                      completedSuccessfully = false,
                      reason = t.getMessage
                    ))
              }

          case _: DropSQLStatement =>
            (writeCoordinator ? DropMetric(statement.db, statement.namespace, statement.metric))
              .mapTo[MetricDropped]
              .map(
                x =>
                  SQLStatementResponse(db = x.db,
                                       namespace = x.namespace,
                                       metric = x.metric,
                                       completedSuccessfully = true,
                                       records = Seq.empty))
              .recoverWith {
                case t =>
                  Future.successful(
                    SQLStatementResponse(
                      db = requestDb,
                      namespace = requestNamespace,
                      completedSuccessfully = false,
                      reason = t.getMessage
                    ))
              }
        }

      //Parsing Failure
      case SqlStatementParserFailure(_, error) =>
        Future.successful(
          SQLStatementResponse(db = request.db,
                               namespace = request.namespace,
                               completedSuccessfully = false,
                               reason = "sql statement not valid",
                               message = error)
        )
    }
  }
}
