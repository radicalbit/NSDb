package io.radicalbit.nsdb.client.rpc

import io.grpc.ManagedChannelBuilder
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.requestCommand.{DescribeMetric, ShowMetrics}
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseCommand.{MetricSchemaRetrieved, MetricsGot}
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse
import io.radicalbit.nsdb.rpc.service.{NSDBServiceCommandGrpc, NSDBServiceSQLGrpc}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class GRPCClient(host: String, port: Int) {

  private val log = LoggerFactory.getLogger(classOf[GRPCClient])

  private val channel     = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build
  private val stubSql     = NSDBServiceSQLGrpc.stub(channel)
  private val stubCommand = NSDBServiceCommandGrpc.stub(channel)

  def write(request: RPCInsert): Future[RPCInsertResult] = {
    log.info("Preparing a write request for {}...", request)
    stubSql.insertBit(request)
  }

  def executeSQLStatement(request: SQLRequestStatement): Future[SQLStatementResponse] = {
    log.info("Preparing execution of SQL request: {} ", request.statement)
    stubSql.executeSQLStatement(request)
  }

  def showMetrics(request: ShowMetrics): Future[MetricsGot] = {
    log.info("Preparing of command show metrics for namespace: {} ", request.namespace)
    stubCommand.showMetrics(request)
  }

  def describeMetrics(request: DescribeMetric): Future[MetricSchemaRetrieved] = {
    log.info("Preparing of command describe metric for namespace: {} ", request.namespace)
    stubCommand.describeMetric(request)
  }
}
