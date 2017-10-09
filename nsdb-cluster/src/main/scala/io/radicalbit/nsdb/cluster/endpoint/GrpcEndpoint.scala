package io.radicalbit.nsdb.cluster.endpoint

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import io.radicalbit.nsdb.client.rpc.GRPCServer
import io.radicalbit.nsdb.rpc.request.{Dimension, RPCInsert}
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.service.NSDBServiceGrpc
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.coordinator.WriteCoordinator.{InputMapped, MapInput}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class GrpcEndpoint(readCoordinator: ActorRef, writeCoordinator: ActorRef)(implicit system: ActorSystem)
    extends GRPCServer {

  private val log = LoggerFactory.getLogger(classOf[GrpcEndpoint])

  implicit val timeout: Timeout =
    Timeout(system.settings.config.getDuration("nsdb.rpc-endpoint.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  implicit val sys = system.dispatcher

  log.info("Starting GrpcEndpoint")

  override protected[this] val executionContextExecutor = implicitly[ExecutionContext]

  override protected[this] def service = GrpcEndpointService

  override protected[this] val port: Int = 7817

  val innerServer = start()

  log.debug("GrpcEndpoint started on port {}", port)

  protected[this] object GrpcEndpointService extends NSDBServiceGrpc.NSDBService {

    override def insertBit(request: RPCInsert): Future[RPCInsertResult] = {
      log.debug("Received a write request {}", request)

      val res = (writeCoordinator ? MapInput(
        namespace = request.namespace,
        metric = request.metric,
        ts = request.timestamp,
        record = Bit(timestamp = request.timestamp, dimensions = request.dimensions.map {
          case (k, v) => (k, dimensionFor(v.value))
        }, value = valueFor(request.value))
      )).mapTo[InputMapped] map (_ => RPCInsertResult(true)) recover {
        case t => RPCInsertResult(false, t.getMessage)
      }

      log.debug("Completed the write request {}", request)
      log.debug("The result is {}", res)

      res
    }

    private def valueFor(v: RPCInsert.Value): JSerializable = v match {
      case _: RPCInsert.Value.DecimalValue => v.decimalValue.get
      case _: RPCInsert.Value.LongValue    => v.longValue.get
    }

    private def dimensionFor(v: Dimension.Value): JSerializable = v match {
      case _: Dimension.Value.DecimalValue => v.decimalValue.get
      case _: Dimension.Value.LongValue    => v.longValue.get
      case _: Dimension.Value.StringValue  => v.stringValue.get
    }
  }

}
