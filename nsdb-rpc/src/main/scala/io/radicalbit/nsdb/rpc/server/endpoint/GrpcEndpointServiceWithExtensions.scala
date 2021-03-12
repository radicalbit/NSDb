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

package io.radicalbit.nsdb.rpc.server.endpoint

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import io.grpc.Context
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.extension.{HookResult, NSDbExtension}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.rpc.GrpcBitConverters.GrpcBitConverter
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import io.radicalbit.nsdb.rpc.server.GRPCService
import io.radicalbit.nsdb.rpc.serviceWithExtensions.NSDBServiceWithExtensionGrpc.NSDBServiceWithExtension
import io.radicalbit.nsdb.rpc.serviceWithExtensions.RPCInsertWithExtension
import org.slf4j.LoggerFactory
import scalapb.descriptors.ServiceDescriptor

import scala.concurrent.Future

/**
  * Concrete implementation of the Grpc service With Extension
  */
class GrpcEndpointServiceWithExtensions(writeCoordinator: ActorRef)(implicit timeout: Timeout, system: ActorSystem)
    extends NSDBServiceWithExtension
    with GRPCService {

  implicit val executionContext = system.dispatcher

  private val log = LoggerFactory.getLogger(classOf[GrpcEndpointServiceWithExtensions])

  override def serviceDescriptor: ServiceDescriptor = this.serviceCompanion.scalaDescriptor

  override def insertBitWithExtension(request: RPCInsertWithExtension): Future[RPCInsertResult] = {
    log.error(s"Received a write request $request")
    val securityPayload = getSecurityPaylaod(Context.current())
    log.error(s"Security payload $securityPayload")

    val results: Future[(HookResult, RPCInsertResult)] =
      for {
        bit <- request.bit.fold(Future.failed[Bit](new IllegalArgumentException("bit is required")))(grpcBit =>
          Future(grpcBit.asBit))

        hookResult <- {
          if (NSDbExtension(system).extensionsNames.isEmpty)
            Future.successful(HookResult.Failure("no extensions configured"))
          else
            NSDbExtension(system).insertBitHook(system,
                                                securityPayload,
                                                request.database,
                                                request.namespace,
                                                request.metric,
                                                bit)
        }

        insertBitRes <- {
          (request.persist, hookResult.isSuccess) match {
            case (true, true) =>
              (writeCoordinator ? MapInput(
                db = request.database,
                namespace = request.namespace,
                metric = request.metric,
                ts = bit.timestamp,
                record = bit
              )).map {
                case _: InputMapped =>
                  RPCInsertResult(completedSuccessfully = true)
                case msg: RecordRejected =>
                  RPCInsertResult(completedSuccessfully = false, errors = msg.reasons.mkString(","))
                case _ => RPCInsertResult(completedSuccessfully = false, errors = "unknown reason")
              } recover {
                case t =>
                  log.error(s"error while inserting $bit", t)
                  RPCInsertResult(completedSuccessfully = false, t.getMessage)
              }
            case (false, true) => Future.successful(RPCInsertResult(true))
            case (_)           => Future.successful(RPCInsertResult(false, hookResult.getFailureReason))
          }
        }

      } yield (hookResult, insertBitRes)

    results
      .map {
        case (hookResult: HookResult, insertResult: RPCInsertResult) =>
          log.debug("Completed the write request {}", request)
          val overallResult = hookResult.isSuccess && insertResult.completedSuccessfully
          val overallErrors = Seq(hookResult.getFailureReason, insertResult.errors).mkString(",")
          if (overallResult) {
            log.debug("The results from hooks is {}", hookResult)
            log.debug("The insert results is {}", insertResult)
          } else {
            log.error("The results from hooks is {}", hookResult)
            log.error("The insert results is {}", insertResult)
          }
          RPCInsertResult(overallResult, overallErrors)
      }
      .recover {
        case t: Throwable =>
          log.error(s"error on request $request", t)
          RPCInsertResult(false, t.getMessage)
      }
  }

}
