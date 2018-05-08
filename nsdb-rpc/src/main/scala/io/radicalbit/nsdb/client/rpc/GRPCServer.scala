/*
 * Copyright 2018 Radicalbit S.r.l.
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

import io.grpc.{Server, ServerBuilder}
import io.radicalbit.nsdb.rpc.dump.DumpGrpc
import io.radicalbit.nsdb.rpc.dump.DumpGrpc.Dump
import io.radicalbit.nsdb.rpc.health.HealthGrpc
import io.radicalbit.nsdb.rpc.health.HealthGrpc.Health
import io.radicalbit.nsdb.rpc.service.NSDBServiceCommandGrpc.NSDBServiceCommand
import io.radicalbit.nsdb.rpc.service.NSDBServiceSQLGrpc.NSDBServiceSQL
import io.radicalbit.nsdb.rpc.service.{NSDBServiceCommandGrpc, NSDBServiceSQLGrpc}
import io.radicalbit.nsdb.sql.parser.SQLStatementParser

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * GRPCServer interface. It contains all the services exposed
  */
trait GRPCServer {

  protected[this] def executionContextExecutor: ExecutionContext

  protected[this] def port: Int

  protected[this] def serviceSQL: NSDBServiceSQL

  protected[this] def serviceCommand: NSDBServiceCommand

  protected[this] def health: Health

  protected[this] def dump: Dump

  protected[this] def parserSQL: SQLStatementParser

  sys.addShutdownHook {
    System.err.println("Shutting down gRPC server since JVM is shutting down")
    close()
    System.err.println("Server shut down")
  }

  lazy val server = ServerBuilder
    .forPort(port)
    .addService(NSDBServiceSQLGrpc.bindService(serviceSQL, executionContextExecutor))
    .addService(NSDBServiceCommandGrpc.bindService(serviceCommand, executionContextExecutor))
    .addService(HealthGrpc.bindService(health, executionContextExecutor))
    .addService(DumpGrpc.bindService(dump, executionContextExecutor))
    .build

  def start(): Try[Server] = Try(server.start())

  def close(): Try[Server] = Try(server.shutdown())
}
