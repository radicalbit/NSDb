package io.radicalbit.nsdb.web.routes

import javax.ws.rs.Path

import akka.actor.ActorRef
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError, NotFound}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import akka.pattern.ask
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.swagger.annotations._
import org.json4s.DefaultFormats
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

@ApiModel(description = "Filter sealed trait ",
subTypes = Array(classOf[FilterNullableValue], classOf[FilterByValue])
)
sealed trait Filter

case object Filter {
  def unapply(arg: Filter): Option[(String, Option[JSerializable], String)] =
    arg match {
      case byValue: FilterByValue             => Some((byValue.dimension, Some(byValue.value), byValue.operator.toString))
      case nullableValue: FilterNullableValue => Some((nullableValue.dimension, None, nullableValue.operator.toString))
    }
}

@ApiModel(description = "Filter using operator", parent = classOf[Filter])
case class FilterByValue(
  @(ApiModelProperty @field)(value = "dimension on which apply condition ")
    dimension: String,
  @(ApiModelProperty @field)(value = "value of comparation")
    value: JSerializable,
  @(ApiModelProperty @field)(value = "filter comparison operator", dataType = "io.radicalbit.nsdb.web.routes.FilterOperators" )
    operator: FilterOperators.Value
) extends Filter

@ApiModel(description = "Filter for nullable ", parent = classOf[Filter])
case class FilterNullableValue(
  @(ApiModelProperty @field)(value = "dimension on which apply condition ")
    dimension: String,
  @(ApiModelProperty @field)(value = "filter nullability operator", dataType = "io.radicalbit.nsdb.web.routes.NullableOperators")
    operator: NullableOperators.Value
) extends Filter

@ApiModel(description = "Query body")
case class QueryBody(
  @(ApiModelProperty @field)(value = "database name ")
  db: String,
  @(ApiModelProperty @field)(value = "namespace name ")
  namespace: String,
  @(ApiModelProperty @field)(value = "metric name ")
  metric: String,
  @(ApiModelProperty @field)(value = "sql query string")
  queryString: String,
  @(ApiModelProperty @field)(value = "timestamp lower bound condition", required = false, dataType = "long")
  from: Option[Long],
  @(ApiModelProperty @field)(value = "timestamp upper bound condition", required = false, dataType = "long")
  to: Option[Long],
  @(ApiModelProperty @field)(value = "filters definition, adding where condition", required = false, dataType = "list[io.radicalbit.nsdb.web.routes.Filter]")
  filters: Option[Seq[Filter]])
    extends Metric

@Api(value = "/query", produces = "application/json")
@Path("/query")
trait QueryApi {

  import io.radicalbit.nsdb.web.Formats._

  def readCoordinator: ActorRef
  def writeCoordinator: ActorRef
  def publisherActor: ActorRef
  def authenticationProvider: NSDBAuthProvider

  implicit val timeout: Timeout
  implicit val formats: DefaultFormats

  @ApiModel(description = "Query Response")
  case class QueryResponse(
      @(ApiModelProperty @field)(value = "query result as a Seq of Bits ") records: Seq[Bit]
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
  def queryApi: Route = {
    pathPrefix("query") {
      post {
        entity(as[QueryBody]) { qb =>
          optionalHeaderValueByName(authenticationProvider.headerName) { header =>
            authenticationProvider.authorizeMetric(ent = qb, header = header, writePermission = false) {
              val statementOpt =
                (new SQLStatementParser().parse(qb.db, qb.namespace, qb.queryString), qb.from, qb.to, qb.filters) match {
                  case (Success(statement: SelectSQLStatement), Some(from), Some(to), Some(filters))
                      if filters.nonEmpty =>
                    Some(
                      statement
                        .enrichWithTimeRange("timestamp", from, to)
                        .addConditions(filters.map(f => Filter.unapply(f).get)))
                  case (Success(statement: SelectSQLStatement), None, None, Some(filters)) if filters.nonEmpty =>
                    Some(
                      statement
                        .addConditions(filters.map(f => Filter.unapply(f).get)))
                  case (Success(statement: SelectSQLStatement), Some(from), Some(to), _) =>
                    Some(
                      statement
                        .enrichWithTimeRange("timestamp", from, to))
                  case (Success(statement: SelectSQLStatement), _, _, _) =>
                    Some(statement)
                  case _ => None
                }
              statementOpt match {
                case Some(statement) =>
                  onComplete(readCoordinator ? ExecuteStatement(statement)) {
                    case Success(SelectStatementExecuted(_, _, _, values)) =>
                      complete(HttpEntity(ContentTypes.`application/json`, write(QueryResponse(values))))
                    case Success(SelectStatementFailed(reason, MetricNotFound(metric))) =>
                      complete(HttpResponse(NotFound, entity = reason))
                    case Success(SelectStatementFailed(reason, _)) =>
                      complete(HttpResponse(InternalServerError, entity = reason))
                    case Success(_) =>
                      complete(HttpResponse(InternalServerError, entity = "unknown response"))
                    case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
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
