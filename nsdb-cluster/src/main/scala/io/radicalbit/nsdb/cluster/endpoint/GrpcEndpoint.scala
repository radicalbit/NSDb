package io.radicalbit.nsdb.cluster.endpoint

import akka.actor.{ActorRef, ActorSystem}
import io.radicalbit.nsdb.client.rpc.GRPCServer
import io.radicalbit.nsdb.rpc.request.RPCInsert
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.service.NSDBServiceGrpc
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.coordinator.WriteCoordinator.{InputMapped, MapInput}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class GrpcEndpoint(readCoordinator: ActorRef, writeCoordinator: ActorRef)(implicit system: ActorSystem)
    extends GRPCServer {

  private val log = LoggerFactory.getLogger(classOf[GrpcEndpoint])

  implicit val timeout: Timeout = 1 second
  implicit val sys              = system.dispatcher

  log.info("Starting GrpcEndpoint")

  override protected[this] val executionContextExecutor = implicitly[ExecutionContext]

  override protected[this] def service = GrpcEndpointService

  override protected[this] val port: Int = 7817

  val innerServer = start()

  log.info("GrpcEndpoint started on port {}", port)

  protected[this] object GrpcEndpointService extends NSDBServiceGrpc.NSDBService {

    override def insertBit(request: RPCInsert): Future[RPCInsertResult] = {
      log.info("Received a write request {}", request)

      val res = (writeCoordinator ? MapInput(
        namespace = request.namespace,
        metric = request.metric,
        ts = request.timestamp,
        record =
          Bit(timestamp = request.timestamp, dimensions = Map.empty[String, JSerializable], metric = request.value)
      )).mapTo[InputMapped] map (_ => RPCInsertResult(true)) recover {
        case t => RPCInsertResult(false, t.getMessage)
      }

      log.info("Completed the write request {}", request)
      log.info("The result is {}", res)

      res
    }
  }
}
