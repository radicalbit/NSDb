package io.radicalbit.nsdb.web.routes

import javax.ws.rs.Path

import akka.actor.ActorRef
import akka.pattern.ask
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{InputMapped, RecordRejected}
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.security.model.Metric
import io.swagger.annotations._
import org.json4s.DefaultFormats

import scala.annotation.meta.field
import scala.util.{Failure, Success}

@ApiModel(description = "Data insertion body")
case class InsertBody(
  @(ApiModelProperty @field)(value = "database name")
  db: String,
  @(ApiModelProperty @field)(value = "namespace name")
  namespace: String,
  @(ApiModelProperty @field)(value = "metric name")
  metric: String,
  @(ApiModelProperty @field)(value = "bit representing a single row", example = """ {"timestamp": 1517333868363,
                                                                                              "value": 23,
                                                                                              "dimensions": {
                                                                                                 "name": "john",
                                                                                                 "surname": "Wane"
                                                                                            }
                                                                                   }""")
  bit: Bit) extends Metric

@Api(value = "/data", produces = "application/json")
@Path("/data")
trait DataApi {

    import io.radicalbit.nsdb.web.Formats._

    def writeCoordinator: ActorRef
    def authenticationProvider: NSDBAuthProvider

    implicit val timeout: Timeout
    implicit val formats: DefaultFormats

    @ApiOperation(value = "Insert Bit", nickname = "insert", httpMethod = "POST", response = classOf[String])
    @ApiImplicitParams(
        Array(
            new ApiImplicitParam(name = "body",
                value = "bit definition",
                required = true,
                dataTypeClass = classOf[InsertBody],
                paramType = "body")
        ))
    @ApiResponses(
        Array(
            new ApiResponse(code = 500, message = "Internal server error"),
            new ApiResponse(code = 400, message = "insert statement is invalid")
        ))
    def dataApi: Route =
        pathPrefix("data") {
            post {
                entity(as[InsertBody]) { insertBody =>
                    optionalHeaderValueByName(authenticationProvider.headerName) { header =>
                        authenticationProvider.authorizeMetric(ent = insertBody, header = header, writePermission = true) {
                            onComplete(
                                writeCoordinator ? MapInput(insertBody.bit.timestamp,
                                    insertBody.db,
                                    insertBody.namespace,
                                    insertBody.metric,
                                    insertBody.bit)) {
                                case Success(_: InputMapped) =>
                                    complete("OK")
                                case Success(RecordRejected(_, _, _, _, reasons)) =>
                                    complete(HttpResponse(BadRequest, entity = reasons.mkString(",")))
                                case Success(_) =>
                                    complete(HttpResponse(InternalServerError, entity = "unknown response"))
                                case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                            }
                        }
                    }
                }
            }
        }

}
