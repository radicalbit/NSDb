package io.radicalbit.nsdb.client.rpc

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import io.radicalbit.nsdb.rpc.dump._
import io.radicalbit.nsdb.rpc.health.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.requestCommand.{DescribeMetric, ShowMetrics, ShowNamespaces}
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseCommand.{MetricSchemaRetrieved, MetricsGot, Namespaces}
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse
import io.radicalbit.nsdb.rpc.service.{NSDBServiceCommandGrpc, NSDBServiceSQLGrpc}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/**
  * Grpc client
  * @param host Grpc server host
  * @param port Grpc server port
  */
class GRPCClient(host: String, port: Int) {

  private val log = LoggerFactory.getLogger(classOf[GRPCClient])

  private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build
  private val stubHealth              = HealthGrpc.stub(channel)
  private val stubDump                = DumpGrpc.stub(channel)
  private val stubSql                 = NSDBServiceSQLGrpc.stub(channel)
  private val stubCommand             = NSDBServiceCommandGrpc.stub(channel)

  def checkConnection(): Future[HealthCheckResponse] = {
    log.debug("checking connection")
    stubHealth.check(HealthCheckRequest("whatever"))
  }

  def createDump(request: DumpRequest): Future[DumpResponse] = {
    log.debug("creating dump")
    stubDump.createDump(request)
  }

  def restore(request: RestoreRequest): Future[RestoreResponse] = {
    log.debug("creating dump")
    stubDump.restore(request)
  }

  def write(request: RPCInsert): Future[RPCInsertResult] = {
    log.debug("Preparing a write request for {}...", request)
    stubSql.insertBit(request)
  }

  def executeSQLStatement(request: SQLRequestStatement): Future[SQLStatementResponse] = {
    log.debug("Preparing execution of SQL request: {} ", request.statement)
    stubSql.executeSQLStatement(request)
  }

  def showNamespaces(request: ShowNamespaces): Future[Namespaces] = {
    log.debug("Preparing of command show namespaces")
    stubCommand.showNamespaces(request)
  }

  def showMetrics(request: ShowMetrics): Future[MetricsGot] = {
    log.debug("Preparing of command show metrics for namespace: {} ", request.namespace)
    stubCommand.showMetrics(request)
  }

  def describeMetrics(request: DescribeMetric): Future[MetricSchemaRetrieved] = {
    log.debug("Preparing of command describe metric for namespace: {} ", request.namespace)
    stubCommand.describeMetric(request)
  }
}
