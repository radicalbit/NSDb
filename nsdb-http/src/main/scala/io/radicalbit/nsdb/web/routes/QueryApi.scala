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
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError, NotFound}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.common.NSDbType
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.SQLStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
import io.radicalbit.nsdb.web.QueryEnriched
import io.swagger.annotations._
import javax.ws.rs.Path
import org.json4s.Formats
import org.json4s.jackson.Serialization.write

import scala.annotation.meta.field
import scala.util.{Failure, Success}

@ApiModel(description = "Filter Operators enumeration with [=, >, >=, <, <=, LIKE]")
object FilterOperators extends Enumeration {
  val Equality       = Value("=")
  val GreaterThan    = Value(">")
  val GreaterOrEqual = Value(">=")
  val LessThan       = Value("<")
  val LessOrEqual    = Value("<=")
  val Like           = Value("LIKE")
}

@ApiModel(description = "Filter Nullability Operators enumeration with [ISNULL, ISNOTNULL]")
object NullableOperators extends Enumeration {
  val IsNull    = Value("ISNULL")
  val IsNotNull = Value("ISNOTNULL")
}

@ApiModel(description = "Filter sealed trait", subTypes = Array(classOf[FilterNullableValue], classOf[FilterByValue]))
sealed trait Filter

case object Filter {
  def unapply(arg: Filter): Option[(String, Option[NSDbType], String)] =
    arg match {
      case byValue: FilterByValue             => Some((byValue.dimension, Some(NSDbType(byValue.value)), byValue.operator.toString))
      case nullableValue: FilterNullableValue => Some((nullableValue.dimension, None, nullableValue.operator.toString))
    }
}

@ApiModel(description = "Filter using operator", parent = classOf[Filter])
case class FilterByValue(
    @(ApiModelProperty @field)(value = "dimension on which apply condition") dimension: String,
    @(ApiModelProperty @field)(value = "value of comparation") value: java.io.Serializable,
    @(ApiModelProperty @field)(
      value = "filter comparison operator",
      dataType = "io.radicalbit.nsdb.web.routes.FilterOperators") operator: FilterOperators.Value
) extends Filter

@ApiModel(description = "Filter for nullable", parent = classOf[Filter])
case class FilterNullableValue(
    @(ApiModelProperty @field)(value = "dimension on which apply condition") dimension: String,
    @(ApiModelProperty @field)(
      value = "filter nullability operator",
      dataType = "io.radicalbit.nsdb.web.routes.NullableOperators") operator: NullableOperators.Value
) extends Filter

@ApiModel(description = "Query body")
case class QueryBody(@(ApiModelProperty @field)(value = "database name") db: String,
                     @(ApiModelProperty @field)(value = "namespace name") namespace: String,
                     @(ApiModelProperty @field)(value = "metric name") metric: String,
                     @(ApiModelProperty @field)(value = "sql query string") queryString: String,
                     @(ApiModelProperty @field)(value = "timestamp lower bound condition",
                                                required = false,
                                                dataType = "long") from: Option[Long],
                     @(ApiModelProperty @field)(value = "timestamp upper bound condition",
                                                required = false,
                                                dataType = "long") to: Option[Long],
                     @(ApiModelProperty @field)(
                       value = "filters definition, adding where condition",
                       required = false,
                       dataType = "list[io.radicalbit.nsdb.web.routes.Filter]") filters: Option[Seq[Filter]],
                     @(ApiModelProperty @field)(value = "return parsed query", required = false, dataType = "boolean") parsed: Option[
                       Boolean])
    extends Metric

@Api(value = "/query", produces = "application/json")
@Path("/query")
trait QueryApi {

  import io.radicalbit.nsdb.web.Formats._

  def readCoordinator: ActorRef
  def authenticationProvider: NSDBAuthProvider

  implicit val timeout: Timeout

  @ApiModel(description = "Query Response")
  case class QueryResponse(
      @(ApiModelProperty @field)(value = "query result as a Seq of Bits") records: Seq[Bit],
      @(ApiModelProperty @field)(value = "json representation of query", required = false, dataType = "SQLStatement") parsed: Option[
        SQLStatement]
  )

  @ApiOperation(value = "Perform query", nickname = "query", httpMethod = "POST", response = classOf[QueryResponse])
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "body",
                           value = "query definition",
                           required = true,
                           dataTypeClass = classOf[QueryBody],
                           paramType = "body")
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 500, message = "Internal server error"),
      new ApiResponse(code = 404, message = "Not found item reason"),
      new ApiResponse(code = 500, message = "Internal server error"),
      new ApiResponse(code = 400, message = "statement is invalid")
    ))
  def queryApi()(implicit logger: LoggingAdapter, format: Formats): Route = {
    path("query") {
      post {
        entity(as[QueryBody]) { qb =>
          optionalHeaderValueByName(authenticationProvider.headerName) { header =>
            authenticationProvider.authorizeMetric(ent = qb, header = header, writePermission = false) {
              QueryEnriched(qb.db, qb.namespace, qb.queryString, qb.from, qb.to, qb.filters.getOrElse(Seq.empty)) match {
                case Some(statement) =>
                  onComplete(readCoordinator ? ExecuteStatement(statement)) {
                    case Success(SelectStatementExecuted(_, values)) =>
                      complete(HttpEntity(ContentTypes.`application/json`,
                                          write(QueryResponse(values, qb.parsed.map(_ => statement)))))
                    case Success(SelectStatementFailed(_, reason, MetricNotFound(metric))) =>
                      complete(HttpResponse(NotFound, entity = reason))
                    case Success(SelectStatementFailed(_, reason, _)) =>
                      complete(HttpResponse(InternalServerError, entity = reason))
                    case Success(r) =>
                      logger.error("unknown response received {}", r)
                      complete(HttpResponse(InternalServerError, entity = "unknown response"))
                    case Failure(ex) =>
                      logger.error(ex, s"error while trying to execute $statement")
                      complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                  }
                case None => complete(HttpResponse(BadRequest, entity = s"statement ${qb.queryString} is invalid"))
              }
            }
          }
        }
      }
    }
  }
}
