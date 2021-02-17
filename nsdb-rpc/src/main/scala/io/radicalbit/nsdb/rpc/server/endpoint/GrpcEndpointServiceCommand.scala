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
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetMetricInfo, GetMetrics, GetNamespaces, GetSchema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{MetricInfoGot, MetricsGot, NamespacesGot, SchemaGot}
import io.radicalbit.nsdb.rpc.requestCommand.{DescribeMetric, ShowMetrics, ShowNamespaces}
import io.radicalbit.nsdb.rpc.responseCommand.DescribeMetricResponse.MetricField.{FieldClassType => GrpcFieldClassType}
import io.radicalbit.nsdb.rpc.responseCommand.DescribeMetricResponse.{MetricInfo => GrpcMetricInfo}
import io.radicalbit.nsdb.rpc.responseCommand.{
  Namespaces,
  DescribeMetricResponse => GrpcDescribeMetricResponse,
  MetricsGot => GrpcMetricsGot
}
import io.radicalbit.nsdb.rpc.server.GRPCService
import io.radicalbit.nsdb.rpc.service.NSDBServiceCommandGrpc.NSDBServiceCommand
import org.slf4j.LoggerFactory
import scalapb.descriptors.ServiceDescriptor

import scala.concurrent.{ExecutionContext, Future}

/**
  * Concrete implementation of the command Grpc service
  */
class GrpcEndpointServiceCommand(metadataCoordinator: ActorRef, readCoordinator: ActorRef)(
    implicit timeout: Timeout,
    executionContext: ExecutionContext)
    extends NSDBServiceCommand
    with GRPCService {

  private val log = LoggerFactory.getLogger(classOf[GrpcEndpointServiceCommand])

  override def serviceDescriptor: ServiceDescriptor = this.serviceCompanion.scalaDescriptor

  override def showMetrics(
      request: ShowMetrics
  ): Future[GrpcMetricsGot] = {
    log.debug("Received command ShowMetrics for namespace {}", request.namespace)
    (metadataCoordinator ? GetMetrics(request.db, request.namespace)).map {
      case MetricsGot(db, namespace, metrics) =>
        GrpcMetricsGot(db, namespace, metrics.toList, completedSuccessfully = true)
      case _ =>
        GrpcMetricsGot(request.db, request.namespace, List.empty, completedSuccessfully = false, "Unknown server error")
    }
  }

  override def describeMetric(request: DescribeMetric): Future[GrpcDescribeMetricResponse] = {

    def extractField(schema: Option[Schema]): Iterable[MetricField] =
      schema
        .map(_.fieldsMap.map {
          case (_, field) =>
            MetricField(name = field.name,
                        fieldClassType = field.fieldClassType,
                        `type` = field.indexType.getClass.getSimpleName)
        })
        .getOrElse(Iterable.empty[MetricField])

    def extractFieldClassType(f: MetricField): GrpcFieldClassType = {
      f.fieldClassType match {
        case DimensionFieldType => GrpcFieldClassType.DIMENSION
        case TagFieldType       => GrpcFieldClassType.TAG
        case TimestampFieldType => GrpcFieldClassType.TIMESTAMP
        case ValueFieldType     => GrpcFieldClassType.VALUE
      }
    }

    //TODO: add failure handling
    log.debug("Received command DescribeMetric for metric {}", request.metric)

    Future
      .sequence(
        Seq(
          readCoordinator ? GetSchema(db = request.db, namespace = request.namespace, metric = request.metric),
          metadataCoordinator ? GetMetricInfo(db = request.db, namespace = request.namespace, metric = request.metric)
        ))
      .map {
        case SchemaGot(db, namespace, metric, schema) :: MetricInfoGot(_, _, _, metricInfoOpt) :: Nil =>
          GrpcDescribeMetricResponse(
            db,
            namespace,
            metric,
            extractField(schema)
              .map(f => GrpcDescribeMetricResponse.MetricField(f.name, extractFieldClassType(f), f.`type`))
              .toSeq,
            metricInfoOpt.map(mi => GrpcMetricInfo(mi.shardInterval, mi.retention)),
            completedSuccessfully = true
          )
        case _ =>
          GrpcDescribeMetricResponse(request.db,
                                     request.namespace,
                                     request.metric,
                                     completedSuccessfully = false,
                                     errors = "unknown message received from server")
      }
  }

  override def showNamespaces(
      request: ShowNamespaces
  ): Future[Namespaces] = {
    log.debug("Received command ShowNamespaces for db: {}", request.db)
    (metadataCoordinator ? GetNamespaces(db = request.db))
      .map {
        case NamespacesGot(db, namespaces) =>
          Namespaces(db = db, namespaces = namespaces.toSeq, completedSuccessfully = true)
        case _ =>
          Namespaces(db = request.db, completedSuccessfully = false, errors = "Cluster unable to fulfill request")
      }
      .recoverWith {
        case _ =>
          Future.successful(
            Namespaces(db = request.db, completedSuccessfully = false, errors = "Cluster unable to fulfill request"))
      }
  }
}
