package io.radicalbit.nsdb.client.rpc

import io.grpc.{Server, ServerBuilder}
import io.radicalbit.nsdb.rpc.service.NSDBServiceGrpc

import scala.concurrent.ExecutionContext
import scala.util.Try

trait GRPCServer {

  protected[this] def executionContextExecutor: ExecutionContext

  protected[this] def port: Int

  protected[this] def service: NSDBServiceGrpc.NSDBService

  sys.addShutdownHook {
    System.err.println("Shutting down gRPC server since JVM is shutting down")
    close()
    System.err.println("Server shut down")
  }

  lazy val server = ServerBuilder
    .forPort(port)
    .addService(NSDBServiceGrpc.bindService(service, executionContextExecutor))
    .build

  def start(): Try[Server] = Try(server.start())

  def close(): Try[Server] = Try(server.shutdown())
}
