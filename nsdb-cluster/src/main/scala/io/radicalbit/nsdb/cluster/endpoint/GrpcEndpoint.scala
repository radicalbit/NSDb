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

package io.radicalbit.nsdb.cluster.endpoint

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{ExecuteRestoreMetadata, PutMetricInfo}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{
  MetadataRestored,
  MetricInfoFailed,
  MetricInfoPut
}
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.rpc.health.HealthCheckResponse.ServingStatus
import io.radicalbit.nsdb.rpc.health.HealthGrpc.Health
import io.radicalbit.nsdb.rpc.health.{HealthCheckRequest, HealthCheckResponse}
import io.radicalbit.nsdb.rpc.init.InitMetricGrpc.InitMetric
import io.radicalbit.nsdb.rpc.init.{InitMetricRequest, InitMetricResponse}
import io.radicalbit.nsdb.rpc.restore.RestoreGrpc.Restore
import io.radicalbit.nsdb.rpc.restore.{RestoreRequest, RestoreResponse}
import io.radicalbit.nsdb.rpc.server.GRPCServer
import io.radicalbit.nsdb.rpc.service.NSDBServiceCommandGrpc.NSDBServiceCommand
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
  * Concrete implementation of NSDb's Grpc endpoint
  * @param readCoordinator the read coordinator actor
  * @param writeCoordinator the write coordinator actor
  * @param system the global actor system
  */
class GrpcEndpoint(nodeId: String,
                   readCoordinator: ActorRef,
                   writeCoordinator: ActorRef,
                   metadataCoordinator: ActorRef,
                   override val authorizationProvider: NSDbAuthorizationProvider)(implicit system: ActorSystem)
    extends GRPCServer {

  private val log = LoggerFactory.getLogger(classOf[GrpcEndpoint])

  implicit val timeout: Timeout =
    Timeout(system.settings.config.getDuration("nsdb.rpc-endpoint.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  implicit val dispatcher: ExecutionContextExecutor = system.dispatcher

  override protected[this] val executionContextExecutor: ExecutionContext = implicitly[ExecutionContext]

  override protected[this] lazy val serviceSQL =
    new GrpcEndpointServiceSQL(writeCoordinator, readCoordinator, parserSQL)

  override protected[this] lazy val serviceCommand: NSDBServiceCommand =
    new GrpcEndpointServiceCommand(metadataCoordinator, readCoordinator)

  override protected[this] lazy val initMetricService: InitMetric = InitMetricService

  override protected[this] lazy val health: Health = GrpcEndpointServiceHealth

  override protected[this] lazy val restore: Restore = GrpcEndpointServiceRestore

  override protected[this] val interface: String = system.settings.config.getString(GrpcInterface)

  override protected[this] val port: Int = system.settings.config.getInt(GrpcPort)

  override protected[this] val parserSQL = new SQLStatementParser

  start() match {
    case Success(_) =>
      log.info(s"GrpcEndpoint started for node $nodeId on interface $interface on port $port")
      system.registerOnTermination {
        log.error(
          s"Shutting down gRPC server for node $nodeId on interface $interface on port $port since Actor System is shutting down",
          interface,
          port)
        stop()
        log.error(s"Server for node $nodeId on interface $interface on port $port shut down")
      }
    case Failure(ex) =>
      log.error(s"error in starting Grpc endpoint for node $nodeId on interface $interface on port $port", ex)
  }

  /**
    * Concrete implementation of the health check Grpc service
    */
  protected[this] object GrpcEndpointServiceHealth extends Health {
    override def check(request: HealthCheckRequest): Future[HealthCheckResponse] =
      Future.successful(HealthCheckResponse(ServingStatus.SERVING))
  }

  protected[this] object GrpcEndpointServiceRestore extends Restore {
    override def restore(request: RestoreRequest): Future[RestoreResponse] = {
      log.info(s"executing restore metadata for request $request")
      (metadataCoordinator ? ExecuteRestoreMetadata(request.sourcePath))
        .map {
          case MetadataRestored(path) => RestoreResponse(completedSuccessfully = true, path)
          case msg =>
            log.error("got {} from restore request", msg)
            RestoreResponse(completedSuccessfully = false,
                            errorMsg = "unknown response received",
                            path = request.sourcePath)
        }
        .recover {
          case t: Throwable =>
            log.error("error occurred in restore metadata", t)
            RestoreResponse(completedSuccessfully = false,
                            errorMsg = s"unknown error occurred ${t.getMessage}",
                            path = request.sourcePath)
        }
    }
  }

  /**
    * Concrete implementation of the init metric service.
    */
  protected[this] object InitMetricService extends InitMetric {
    override def initMetric(request: InitMetricRequest): Future[InitMetricResponse] = {

      Try {
        (if (request.shardInterval.nonEmpty) Duration(request.shardInterval).toMillis else 0L,
         if (request.retention.nonEmpty) Duration(request.retention).toMillis else 0L)
      } match {
        case Success((interval, retention)) =>
          (metadataCoordinator ? PutMetricInfo(
            MetricInfo(request.db, request.namespace, request.metric, interval, retention)))
            .map {
              case MetricInfoPut(_) =>
                InitMetricResponse(request.db, request.namespace, request.metric, completedSuccessfully = true)
              case MetricInfoFailed(_, message) =>
                InitMetricResponse(request.db,
                                   request.namespace,
                                   request.metric,
                                   completedSuccessfully = false,
                                   errorMsg = message)
              case _ =>
                InitMetricResponse(request.db,
                                   request.namespace,
                                   request.metric,
                                   completedSuccessfully = false,
                                   errorMsg = "Unknown response from server")
            }
        case Failure(ex) =>
          Future(
            InitMetricResponse(request.db,
                               request.namespace,
                               request.metric,
                               completedSuccessfully = false,
                               errorMsg = ex.getMessage))
      }
    }
  }

}
