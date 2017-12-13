package io.radicalbit.nsdb.cluster.endpoint

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.client.rpc.GRPCServer
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{InputMapped, MapInput}
import io.radicalbit.nsdb.rpc.common.{Dimension, Bit => GrpcBit}
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.requestCommand.{DescribeMetric, ShowMetrics}
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.responseCommand.{MetricSchemaRetrieved, MetricsGot}
import io.radicalbit.nsdb.rpc.responseSQL.SQLStatementResponse
import io.radicalbit.nsdb.rpc.service.NSDBServiceCommandGrpc.NSDBServiceCommand
import io.radicalbit.nsdb.rpc.service.NSDBServiceSQLGrpc.NSDBServiceSQL
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class GrpcEndpoint(readCoordinator: ActorRef, writeCoordinator: ActorRef)(implicit system: ActorSystem)
    extends GRPCServer {

  private val log = LoggerFactory.getLogger(classOf[GrpcEndpoint])

  implicit val timeout: Timeout =
    Timeout(system.settings.config.getDuration("nsdb.rpc-endpoint.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  implicit val sys = system.dispatcher

  log.info("Starting GrpcEndpoint")

  override protected[this] val executionContextExecutor = implicitly[ExecutionContext]

  override protected[this] def serviceSQL = GrpcEndpointServiceSQL

  override protected[this] def serviceCommand = GrpcEndpointServiceCommand

  override protected[this] val port: Int = 7817

  override protected[this] val parserSQL = new SQLStatementParser

  val innerServer = start()

  log.info("GrpcEndpoint started on port {}", port)

  protected[this] object GrpcEndpointServiceCommand extends NSDBServiceCommand {
    override def showMetrics(
        request: ShowMetrics
    ): Future[MetricsGot] = ???

    override def describeMetric(
        request: DescribeMetric
    ): Future[MetricSchemaRetrieved] = ???
  }

  protected[this] object GrpcEndpointServiceSQL extends NSDBServiceSQL {

    override def insertBit(request: RPCInsert): Future[RPCInsertResult] = {
      log.debug("Received a write request {}", request)

      val res = (writeCoordinator ? MapInput(
        db = request.database,
        namespace = request.namespace,
        metric = request.metric,
        ts = request.timestamp,
        record = Bit(timestamp = request.timestamp, dimensions = request.dimensions.map {
          case (k, v) => (k, dimensionFor(v.value))
        }, value = valueFor(request.value))
      )).mapTo[InputMapped] map (_ => RPCInsertResult(completedSuccessfully = true)) recover {
        case t => RPCInsertResult(completedSuccessfully = false, t.getMessage)
      }

      res.onComplete {
        case Success(res: RPCInsertResult) =>
          log.debug("Completed the write request {}", request)
          if (res.completedSuccessfully)
            log.debug("The result is {}", res)
          else
            log.error("The result is {}", res)
        case Failure(t: Throwable) =>
          log.error(s"error on request $request", t)
      }
      res
    }

    private def valueFor(v: RPCInsert.Value): JSerializable = v match {
      case _: RPCInsert.Value.DecimalValue => v.decimalValue.get
      case _: RPCInsert.Value.LongValue    => v.longValue.get
    }

    private def dimensionFor(v: Dimension.Value): JSerializable = v match {
      case _: Dimension.Value.DecimalValue => v.decimalValue.get
      case _: Dimension.Value.LongValue    => v.longValue.get
      case _                               => v.stringValue.get
    }

    override def executeSQLStatement(
        request: SQLRequestStatement
    ): Future[SQLStatementResponse] = {
      parserSQL.parse(request.db, request.namespace, request.statement)

      ???
    }
  }

}
