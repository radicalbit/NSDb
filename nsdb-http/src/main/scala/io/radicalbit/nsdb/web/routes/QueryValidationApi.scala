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

package io.radicalbit.nsdb.web.routes

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ValidateStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import io.swagger.annotations._
import javax.ws.rs.Path
import org.json4s.Formats

import scala.annotation.meta.field
import scala.util.{Failure, Success}

@ApiModel(description = "Query Validation body")
case class QueryValidationBody(@(ApiModelProperty @field)(value = "database name ") db: String,
                               @(ApiModelProperty @field)(value = "namespace name ") namespace: String,
                               @(ApiModelProperty @field)(value = "metric name ") metric: String,
                               @(ApiModelProperty @field)(value = "sql query string") queryString: String)
    extends Metric

@Api(value = "/query/validate", produces = "application/json")
@Path("/query/validate")
trait QueryValidationApi {

  import io.radicalbit.nsdb.web.NSDbJsonProtocol._

  def readCoordinator: ActorRef
  def authenticationProvider: NSDBAuthProvider

  implicit val timeout: Timeout
  implicit val formats: Formats

  @ApiOperation(value = "Perform query", nickname = "query", httpMethod = "POST", response = classOf[String])
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "body",
                           value = "query definition",
                           required = true,
                           dataTypeClass = classOf[QueryValidationBody],
                           paramType = "body")
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 200, message = "Query is valid"),
      new ApiResponse(code = 404, message = "Not found item reason"),
      new ApiResponse(code = 400, message = "statement is invalid")
    ))
  def queryValidationApi(implicit logger: LoggingAdapter): Route = {
    path("query" / "validate") {
      post {
        entity(as[QueryValidationBody]) { qb =>
          optionalHeaderValueByName(authenticationProvider.headerName) { header =>
            authenticationProvider.authorizeMetric(ent = qb, header = header, writePermission = false) {
              new SQLStatementParser().parse(qb.db, qb.namespace, qb.queryString) match {
                case SqlStatementParserSuccess(_, statement: SelectSQLStatement) =>
                  onComplete(readCoordinator ? ValidateStatement(statement)) {
                    case Success(SelectStatementValidated(_)) =>
                      complete(HttpResponse(OK))
                    case Success(SelectStatementValidationFailed(_, reason, MetricNotFound(_))) =>
                      complete(HttpResponse(NotFound, entity = reason))
                    case Success(SelectStatementValidationFailed(_, reason, _)) =>
                      complete(HttpResponse(BadRequest, entity = reason))
                    case Success(r) =>
                      logger.error("unknown response received {}", r)
                      complete(HttpResponse(InternalServerError, entity = "unknown response"))
                    case Failure(ex) =>
                      logger.error("", ex)
                      complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                  }
                case SqlStatementParserSuccess(queryString, _) =>
                  complete(HttpResponse(BadRequest, entity = s"statement ${queryString} is not a select statement"))
                case SqlStatementParserFailure(queryString, _) =>
                  complete(HttpResponse(BadRequest, entity = s"statement ${queryString} is invalid"))
              }
            }
          }
        }
      }
    }
  }
}
