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

package io.radicalbit.nsdb.rpc.server

import com.google.common.net.InetAddresses
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.{Server, ServerInterceptor, ServerInterceptors}
import io.radicalbit.nsdb.rpc.health.HealthGrpc
import io.radicalbit.nsdb.rpc.health.HealthGrpc.Health
import io.radicalbit.nsdb.rpc.init.InitMetricGrpc
import io.radicalbit.nsdb.rpc.init.InitMetricGrpc.InitMetric
import io.radicalbit.nsdb.rpc.restore.RestoreGrpc
import io.radicalbit.nsdb.rpc.restore.RestoreGrpc.Restore
import io.radicalbit.nsdb.rpc.service.NSDBServiceCommandGrpc.NSDBServiceCommand
import io.radicalbit.nsdb.rpc.service.NSDBServiceSQLGrpc.NSDBServiceSQL
import io.radicalbit.nsdb.rpc.service.{NSDBServiceCommandGrpc, NSDBServiceSQLGrpc}
import io.radicalbit.nsdb.rpc.serviceWithExtensions.NSDBServiceWithExtensionGrpc
import io.radicalbit.nsdb.rpc.serviceWithExtensions.NSDBServiceWithExtensionGrpc.NSDBServiceWithExtension
import io.radicalbit.nsdb.rpc.streaming.NSDbStreamingGrpc
import io.radicalbit.nsdb.rpc.streaming.NSDbStreamingGrpc.NSDbStreaming
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.sql.parser.SQLStatementParser

import java.net.InetSocketAddress
import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * GRPCServer interface. It contains all the services exposed
  */
trait GRPCServer {

  protected[this] def executionContextExecutor: ExecutionContext

  protected[this] def interface: String

  protected[this] def port: Int

  protected[this] def serviceSQL: NSDBServiceSQL

  protected[this] def serviceWithExtension: NSDBServiceWithExtension

  protected[this] def serviceCommand: NSDBServiceCommand

  protected[this] def initMetricService: InitMetric

  protected[this] def health: Health

  protected[this] def restore: Restore

  protected[this] def streamingService: NSDbStreaming

  protected[this] def parserSQL: SQLStatementParser

  protected[this] def authorizationProvider: NSDbAuthorizationProvider

  protected[this] def interceptors: List[ServerInterceptor] =
    if (!authorizationProvider.isEmpty) List(new GrpcAuthInterceptor(authorizationProvider)) else List.empty

  sys.addShutdownHook {
    if (!server.isTerminated) {
      System.err.println(s"Shutting down gRPC server at interface $interface and port $port since JVM is shutting down")
      stop()
      System.err.println(s"Server at interface $interface and port $port shut down")
    }
  }

  lazy val server: Server = NettyServerBuilder
    .forAddress(new InetSocketAddress(InetAddresses.forString(interface), port))
    .addService(ServerInterceptors.intercept(NSDBServiceSQLGrpc.bindService(serviceSQL, executionContextExecutor),
                                             interceptors: _*))
    .addService(ServerInterceptors.intercept(NSDBServiceWithExtensionGrpc.bindService(serviceWithExtension,
                                                                                      executionContextExecutor),
                                             interceptors: _*))
    .addService(ServerInterceptors
      .intercept(NSDBServiceCommandGrpc.bindService(serviceCommand, executionContextExecutor), interceptors: _*))
    .addService(ServerInterceptors.intercept(InitMetricGrpc.bindService(initMetricService, executionContextExecutor),
                                             interceptors: _*))
    .addService(ServerInterceptors.intercept(RestoreGrpc.bindService(restore, executionContextExecutor),
                                             interceptors: _*))
    .addService(NSDbStreamingGrpc.bindService(streamingService, executionContextExecutor))
    .addService(HealthGrpc.bindService(health, executionContextExecutor))
    .addService(ProtoReflectionService.newInstance())
    .build

  def start(): Try[Server] = Try(server.start())

  def stop(): Unit = server.shutdownNow().awaitTermination()
}
