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

package io.radicalbit.nsdb.client.rpc

import io.grpc.stub.AbstractStub

import java.util.concurrent.{Executor, TimeUnit}
import io.grpc.{CallCredentials, ManagedChannel, ManagedChannelBuilder, Metadata, Status}
import io.radicalbit.nsdb.rpc.health.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.radicalbit.nsdb.rpc.init._
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.requestCommand.{DescribeMetric, ShowMetrics, ShowNamespaces}
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseCommand.{DescribeMetricResponse, MetricsGot, Namespaces}
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse
import io.radicalbit.nsdb.rpc.restore.{RestoreGrpc, RestoreRequest, RestoreResponse}
import io.radicalbit.nsdb.rpc.service.{NSDBServiceCommandGrpc, NSDBServiceSQLGrpc}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class JwtTokenApplier(token: String) extends CallCredentials {

  private val log = LoggerFactory.getLogger(classOf[JwtTokenApplier])

  override def applyRequestMetadata(requestInfo: CallCredentials.RequestInfo,
                                    appExecutor: Executor,
                                    applier: CallCredentials.MetadataApplier): Unit = {
    appExecutor.execute(
      () =>
        if (token != null && token.nonEmpty)
          try {
            val metadata = new Metadata
            metadata.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), s"Bearer $token")
            applier.apply(metadata)
          } catch {
            case e: RuntimeException =>
              val errorMsg = "An exception when obtaining JWT token"
              log.error(errorMsg, e)
              applier.fail(Status.UNAUTHENTICATED.withDescription(errorMsg).withCause(e))
          } else
          applier.fail(Status.UNAUTHENTICATED.withDescription("Empty token provided")))
  }

  override def thisUsesUnstableApi(): Unit = {}
}

/**
  * GRPC client
  * @param host GRPC server host
  * @param port GRPC server port
  */
class GRPCClient(host: String, port: Int) {

  private val log = LoggerFactory.getLogger(classOf[GRPCClient])

  private val channel: ManagedChannel           = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
  private val stubHealth: HealthGrpc.HealthStub = HealthGrpc.stub(channel)
  private val stubRestore                       = RestoreGrpc.stub(channel)
  private val stubSql                           = NSDBServiceSQLGrpc.stub(channel)
  private val stubCommand                       = NSDBServiceCommandGrpc.stub(channel)
  private val stubInit                          = InitMetricGrpc.stub(channel)

  private def prepareStub[T <: AbstractStub[T]](stub: T, token: String = "") =
    Option(token).filter(_.trim.nonEmpty).fold(stub)(t => stub.withCallCredentials(new JwtTokenApplier(t)))

  def checkConnection(): Future[HealthCheckResponse] = {
    log.debug("checking connection")
    stubHealth.check(HealthCheckRequest("whatever"))
  }

  def restore(request: RestoreRequest): Future[RestoreResponse] = {
    log.debug("creating dump")
    stubRestore.restore(request)
  }

  def initMetric(request: InitMetricRequest): Future[InitMetricResponse] = {
    log.debug("Preparing a init request for {}", request)
    stubInit.initMetric(request)
  }

  def write(request: RPCInsert): Future[RPCInsertResult] = {
    log.debug("Preparing a write request for {}...", request)
    stubSql.insertBit(request)
  }

  def executeSQLStatement(request: SQLRequestStatement, token: String = ""): Future[SQLStatementResponse] = {
    log.debug("Preparing execution of SQL request: {} ", request.statement)
    prepareStub(stubSql, token).executeSQLStatement(request)
  }

  def showNamespaces(request: ShowNamespaces): Future[Namespaces] = {
    log.debug("Preparing of command show namespaces")
    stubCommand.showNamespaces(request)
  }

  def showMetrics(request: ShowMetrics): Future[MetricsGot] = {
    log.debug("Preparing of command show metrics for namespace: {} ", request.namespace)
    stubCommand.showMetrics(request)
  }

  def describeMetric(request: DescribeMetric): Future[DescribeMetricResponse] = {
    log.debug("Preparing of command describe metric for namespace: {} ", request.namespace)
    stubCommand.describeMetric(request)
  }

  def close(): Unit = channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS)

}
