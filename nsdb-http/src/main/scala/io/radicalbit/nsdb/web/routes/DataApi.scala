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
import org.json4s.Formats

import scala.annotation.meta.field
import scala.util.{Failure, Success}

@ApiModel(description = "Data insertion body")
case class InsertBody(@(ApiModelProperty @field)(value = "database name") db: String,
                      @(ApiModelProperty @field)(value = "namespace name") namespace: String,
                      @(ApiModelProperty @field)(value = "metric name") metric: String,
                      @(ApiModelProperty @field)(
                        value = "bit representing a single row"
                      ) bit: Bit)
    extends Metric

@Api(value = "/data", produces = "application/json")
@Path("/data")
trait DataApi {

  import io.radicalbit.nsdb.web.NSDbJson._
  import io.radicalbit.nsdb.web.validation.ValidationDirective._
  import io.radicalbit.nsdb.web.validation.Validators._

  def writeCoordinator: ActorRef
  def authenticationProvider: NSDBAuthProvider

  implicit val timeout: Timeout
  implicit val formats: Formats

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
            validateModel(insertBody).apply { validatedInsertBody =>
              authenticationProvider.authorizeMetric(ent = validatedInsertBody, header = header, writePermission = true) {
                onComplete(
                  writeCoordinator ? MapInput(validatedInsertBody.bit.timestamp,
                                              validatedInsertBody.db,
                                              validatedInsertBody.namespace,
                                              validatedInsertBody.metric,
                                              validatedInsertBody.bit)) {
                  case Success(_: InputMapped) =>
                    complete("OK")
                  case Success(RecordRejected(_, _, _, _, _, reasons, _)) =>
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

}
