package io.radicalbit.nsdb.client.rpc

import io.grpc.ManagedChannelBuilder
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.service.NSDBServiceGrpc
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class GRPCClient(host: String, port: Int) {

  private val log = LoggerFactory.getLogger(classOf[GRPCClient])

  private val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build
  private val stub    = NSDBServiceGrpc.stub(channel)

  def write(request: RPCInsert): Future[RPCInsertResult] = {
    log.info("Preparing a write request for {}...", request)
    stub.insertBit(request)
  }
}
