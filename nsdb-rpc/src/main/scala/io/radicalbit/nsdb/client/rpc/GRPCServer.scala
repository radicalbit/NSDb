package io.radicalbit.nsdb.client.rpc

import io.grpc.{Server, ServerBuilder}
import io.radicalbit.nsdb.rpc.service.NSDBServiceCommandGrpc.NSDBServiceCommand
import io.radicalbit.nsdb.rpc.service.{NSDBServiceCommandGrpc, NSDBServiceSQLGrpc}
import io.radicalbit.nsdb.rpc.service.NSDBServiceSQLGrpc.NSDBServiceSQL
import io.radicalbit.nsdb.sql.parser.SQLStatementParser

import scala.concurrent.ExecutionContext
import scala.util.Try

trait GRPCServer {

  protected[this] def executionContextExecutor: ExecutionContext

  protected[this] def port: Int

  protected[this] def serviceSQL: NSDBServiceSQL

  protected[this] def serviceCommand: NSDBServiceCommand

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
    .build

  def start(): Try[Server] = Try(server.start())

  def close(): Try[Server] = Try(server.shutdown())
}
