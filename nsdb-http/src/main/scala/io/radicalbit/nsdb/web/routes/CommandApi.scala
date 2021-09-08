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
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, NotFound}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.web.NSDbHttpSecurityDirective.{
  withDbAuthorization,
  withMetricAuthorization,
  withNamespaceAuthorization
}
import io.swagger.annotations._
import org.json4s.Formats
import org.json4s.jackson.Serialization.write

import javax.ws.rs.Path
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@Api(value = "/commands", produces = "application/json")
@Path("/commands")
trait CommandApi {

  def readCoordinator: ActorRef
  def writeCoordinator: ActorRef
  def metadataCoordinator: ActorRef
  def authorizationProvider: NSDbAuthorizationProvider

  implicit val timeout: Timeout
  implicit val formats: Formats
  implicit val ec: ExecutionContext

  case class CommandRequestDatabase(db: String)
  case class CommandRequestNamespace(db: String, namespace: String)
  case class CommandRequestMetric(db: String, namespace: String, metric: String)

  sealed trait CommandResponse
  case class ShowDbsResponse(dbs: Set[String])               extends CommandResponse
  case class ShowNamespacesResponse(namespaces: Set[String]) extends CommandResponse
  case class ShowMetricsResponse(metrics: Set[String])       extends CommandResponse
  case class Field(name: String, `type`: String)
  case class DescribeMetricResponse(fields: Set[Field], metricInfo: Option[MetricInfo]) extends CommandResponse

  @Api(value = "/dbs", produces = "application/json")
  @Path("/dbs")
  @ApiOperation(
    value = "Perform show topology command. A topology is a representation of the cluster members",
    nickname = "show topology",
    httpMethod = "GET",
    response = classOf[TopologyGot]
  )
  @ApiResponses(
    Array(
      new ApiResponse(code = 500, message = "Internal server error")
    ))
  def topologyApi: Route =
    path("topology") {
      (pathEnd & get) {
        onComplete(metadataCoordinator ? GetTopology) {
          case Success(topology: TopologyGot) =>
            complete(HttpEntity(ContentTypes.`application/json`, write(topology)))
          case Success(_)  => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
          case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
        }
      }
    }

  @Api(value = "locations/{db}/{namespace}/{metric}", produces = "application/json")
  @Path("/locations/{db}/{namespace}/{metric}")
  @ApiOperation(value = "Perform show locations given db, namespace, metric",
                nickname = "Show Locations",
                httpMethod = "GET",
                response = classOf[String])
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "db", value = "database", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "namespace",
                           value = "namespace",
                           required = true,
                           dataType = "string",
                           paramType = "path")
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 500, message = "Internal server error")
    ))
  def locationsApi: Route = {
    pathPrefix("locations") {
      pathPrefix(Segment) { db =>
        pathPrefix(Segment) { namespace =>
          pathPrefix(Segment) { metric =>
            (pathEnd & get) {
              withMetricAuthorization(db, namespace, metric, false, authorizationProvider) {
                onComplete(metadataCoordinator ? GetLocations(db, namespace, metric)) {
                  case Success(response: LocationsGot) =>
                    complete(HttpEntity(ContentTypes.`application/json`, write(response)))
                  case Success(_)  => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                  case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                }
              }
            }
          }
        }
      }
    }
  }

  @Api(value = "/dbs", produces = "application/json")
  @Path("/dbs")
  @ApiOperation(value = "Perform show dbs command",
                nickname = "show dbs",
                httpMethod = "GET",
                response = classOf[ShowNamespacesResponse])
  @ApiResponses(
    Array(
      new ApiResponse(code = 500, message = "Internal server error")
    ))
  def showDbs: Route =
    path("dbs") {
      (pathEnd & get) {
        onComplete(readCoordinator ? GetDbs) {
          case Success(DbsGot(dbs)) =>
            complete(HttpEntity(ContentTypes.`application/json`, write(ShowDbsResponse(dbs))))
          case Success(_)  => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
          case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
        }
      }
    }

  @Api(value = "/{db}/namespaces", produces = "application/json")
  @Path("/{db}/namespaces")
  @ApiOperation(value = "Perform show namespaces command",
                nickname = "show namespaces",
                httpMethod = "GET",
                response = classOf[ShowNamespacesResponse])
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "db", value = "database", required = true, dataType = "string", paramType = "path")
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 500, message = "Internal server error")
    ))
  def showNamespaces: Route =
    pathPrefix(Segment) { db =>
      path("namespaces") {
        (pathEnd & get) {
          withDbAuthorization(db, false, authorizationProvider) {
            onComplete(readCoordinator ? GetNamespaces(db)) {
              case Success(NamespacesGot(_, namespaces)) =>
                complete(HttpEntity(ContentTypes.`application/json`, write(ShowNamespacesResponse(namespaces))))
              case Success(_)  => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
              case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
            }
          }
        }
      }
    }

  @Api(value = "/{db}/{namespace}", produces = "application/json")
  @Path("/{db}/{namespace}")
  @ApiOperation(value = "Perform drop namespace command",
                nickname = "Drop namespace",
                httpMethod = "DELETE",
                response = classOf[String])
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "db", value = "database", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "namespace",
                           value = "namespace",
                           required = true,
                           dataType = "string",
                           paramType = "path")
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 500, message = "Internal server error")
    ))
  def dropNamespace: Route = {
    pathPrefix(Segment) { db =>
      pathPrefix(Segment) { namespace =>
        pathEnd {
          delete {
            withNamespaceAuthorization(db, namespace, true, authorizationProvider) {
              onComplete(writeCoordinator ? DeleteNamespace(db, namespace)) {
                case Success(NamespaceDeleted(_, _)) => complete("Ok")
                case Success(_)                      => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                case Failure(ex)                     => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
              }
            }
          }
        }
      }
    }
  }

  @Api(value = "/{db}/{namespace}/metrics", produces = "application/json")
  @Path("/{db}/{namespace}/metrics")
  @ApiOperation(value = "Perform show metrics command",
                nickname = "Show metrics",
                httpMethod = "GET",
                response = classOf[ShowMetricsResponse])
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "db", value = "database", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "namespace",
                           value = "namespace",
                           required = true,
                           dataType = "string",
                           paramType = "path")
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 500, message = "Internal server error")
    ))
  def showMetrics: Route =
    pathPrefix(Segment) { db =>
      pathPrefix(Segment) { namespace =>
        path("metrics") {
          pathEnd {
            get {
              withNamespaceAuthorization(db, namespace, false, authorizationProvider) {
                onComplete(readCoordinator ? GetMetrics(db, namespace)) {
                  case Success(MetricsGot(_, _, metrics)) =>
                    complete(HttpEntity(ContentTypes.`application/json`, write(ShowMetricsResponse(metrics))))
                  case Success(_)  => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                  case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                }
              }
            }
          }
        }
      }
    }

  @Api(value = "/{db}/{namespace}/{metric}", produces = "application/json")
  @Path("/{db}/{namespace}/{metric}")
  @ApiOperation(value = "Perform describe metric command",
                nickname = "Describe metric",
                httpMethod = "GET",
                response = classOf[DescribeMetricResponse])
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "db", value = "database", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "namespace",
                           value = "namespace",
                           required = true,
                           dataType = "string",
                           paramType = "path"),
      new ApiImplicitParam(name = "metric", value = "metric", required = true, dataType = "string", paramType = "path")
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 404, message = "Metric Not Found"),
      new ApiResponse(code = 500, message = "Internal server error")
    ))
  def describeMetric: Route =
    pathPrefix(Segment) { db =>
      pathPrefix(Segment) { namespace =>
        pathPrefix(Segment) { metric =>
          (pathEnd & get) {
            withMetricAuthorization(db, namespace, metric, false, authorizationProvider) {
              onComplete(
                Future.sequence(
                  Seq((readCoordinator ? GetSchema(db, namespace, metric)).mapTo[SchemaGot],
                      (metadataCoordinator ? GetMetricInfo(db, namespace, metric)).mapTo[MetricInfoGot])
                )) {
                case Success(SchemaGot(_, _, _, Some(schema)) :: MetricInfoGot(_, _, _, metricInfo) :: Nil) =>
                  complete(
                    HttpEntity(
                      ContentTypes.`application/json`,
                      write(
                        DescribeMetricResponse(
                          schema.fieldsMap.map {
                            case (_, field) =>
                              Field(name = field.name, `type` = field.indexType.getClass.getSimpleName)
                          }.toSet,
                          metricInfo
                        )
                      )
                    )
                  )
                case Success(SchemaGot(_, _, _, schemaOpt) :: MetricInfoGot(_, _, _, Some(metricInfo)) :: Nil) =>
                  complete(
                    HttpEntity(
                      ContentTypes.`application/json`,
                      write(
                        DescribeMetricResponse(
                          schemaOpt
                            .map(s =>
                              s.fieldsMap.map {
                                case (_, field) =>
                                  Field(name = field.name, `type` = field.indexType.getClass.getSimpleName)
                              }.toSet)
                            .getOrElse(Set.empty),
                          Some(metricInfo)
                        )
                      )
                    )
                  )
                case Success(SchemaGot(_, _, _, None) :: _ :: Nil) =>
                  complete(HttpResponse(NotFound))
                case Failure(ex) => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
                case _           => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
              }
            }
          }
        }
      }
    }

  @Api(value = "/{db}/{namespace}/{metric}", produces = "application/json")
  @Path("/{db}/{namespace}/{metric}")
  @ApiOperation(value = "Perform drop metric command",
                nickname = "Describe metric",
                httpMethod = "DELETE",
                response = classOf[String])
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "db", value = "database", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "namespace",
                           value = "namespace",
                           required = true,
                           dataType = "string",
                           paramType = "path"),
      new ApiImplicitParam(name = "metric", value = "metric", required = true, dataType = "string", paramType = "path")
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 500, message = "Internal server error")
    ))
  def dropMetric: Route =
    pathPrefix(Segment) { db =>
      pathPrefix(Segment) { namespace =>
        pathPrefix(Segment) { metric =>
          delete {
            withMetricAuthorization(db, namespace, metric, true, authorizationProvider) {
              onComplete(writeCoordinator ? DropMetric(db, namespace, metric)) {
                case Success(MetricDropped(_, _, _)) => complete("Ok")
                case Success(_)                      => complete(HttpResponse(InternalServerError, entity = "Unknown reason"))
                case Failure(ex)                     => complete(HttpResponse(InternalServerError, entity = ex.getMessage))
              }
            }
          }
        }
      }
    }

  def commandsApi: Route =
    pathPrefix("commands") {
      topologyApi ~ locationsApi ~ showDbs ~ showNamespaces ~ showMetrics ~ dropNamespace ~ describeMetric ~ dropMetric
    }

}
