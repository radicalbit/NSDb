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

package io.radicalbit.nsdb.rpc.server

import io.grpc._
import io.radicalbit.nsdb.rpc.security.{AuthorizationLevel, SecurityProto}
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider.AuthorizationResponse
import org.slf4j.LoggerFactory
import scalapb.GeneratedMessage
import scalapb.descriptors.{FieldDescriptor, PValue}

/**
  * Implementation of [[ServerInterceptor]] that checks if a message can be authorized.
  * A message must fulfil the following requirements to be authorized:
  * - be and instance of [[GeneratedMessage]]
  * - contain a proper tuple of authorization fields
  *   - (DB), (DB, NAMESPACE), (DB, NAMESPACE, METRIC)
  *   - all other combination will be rejected
  *     {{{
  *       string db = 1 [(AuthorizationField) = DB];
          string namespace = 2 [(AuthorizationField) = NAMESPACE];
          string metric = 3 [(AuthorizationField) = METRIC];
  *     }}}
  *   - return a successful [[AuthorizationResponse]] from [[authProvider]]
  * @param authProvider configured authorization provider
  */
class GrpcAuthInterceptor(authProvider: NSDbAuthorizationProvider) extends ServerInterceptor {

  import GrpcAuthInterceptor._

  private val log = LoggerFactory.getLogger(classOf[GrpcAuthInterceptor])

  override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT],
                                          headers: Metadata,
                                          next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {

    log.debug(s"intercepting call $call with headers $headers")

    val securityPayload =
      headers.get(Metadata.Key.of(authProvider.getGrpcSecurityHeader, Metadata.ASCII_STRING_MARSHALLER))

    log.debug(s"security payload $securityPayload")

    if (Option(securityPayload).forall(_.trim.isEmpty))
      call.close(Status.UNAUTHENTICATED.withDescription(EmptyToken), new Metadata)

    new ForwardingServerCallListener.SimpleForwardingServerCallListener[ReqT](
      Contexts.interceptCall(Context.current().withValue(GRPCService.securityPayloadKey, securityPayload),
                             call,
                             headers,
                             next)) {
      override def onMessage(request: ReqT): Unit = {

        request match {
          case message: GeneratedMessage =>
            val authorizationLevel: Map[AuthorizationLevel, String] = message.toPMessage.value.collect {
              case (fieldDescriptor: FieldDescriptor, fieldValue: PValue)
                  if !fieldDescriptor.getOptions.extension(SecurityProto.authorizationField).isNone =>
                val authLevel: AuthorizationLevel =
                  fieldDescriptor.getOptions.extension(SecurityProto.authorizationField)
                authLevel -> fieldValue.as[String]
            }

            val authorizationResponse = AuthInfo.validate(authorizationLevel) match {
              case Right(DbAuthInto(db)) =>
                log.debug(s"checking db auth from request $request")
                authProvider.checkDbAuth(db, securityPayload, true)
              case Right(NamespaceAuthInto(db, namespace)) =>
                log.debug(s"checking metric auth from request $request")
                authProvider.checkNamespaceAuth(db, namespace, securityPayload, true)
              case Right(MetricAuthInto(db, namespace, metric)) =>
                log.debug(s"checking db namespace from request $request")
                authProvider.checkMetricAuth(db, namespace, metric, securityPayload, true)
              case Left(error) =>
                new AuthorizationResponse(false, error)
            }

            log.debug(s"got authorization response $authorizationResponse from request $request")

            if (authorizationResponse.isSuccess) delegate().onMessage(request)
            else
              call.close(Status.PERMISSION_DENIED.withDescription(authorizationResponse.getFailReason), new Metadata)
          case _ =>
            call.close(Status.FAILED_PRECONDITION.withDescription(s"$WrongMessageType $request"), new Metadata)
        }
      }

      override def onHalfClose(): Unit = {
        try {
          super.onHalfClose()
        } catch {
          case illegalStateException: java.lang.IllegalStateException
              if illegalStateException.getMessage == CallAlreadyClosed => //call has been already closed.no further action required.
        }
      }
    }
  }
}

object GrpcAuthInterceptor {

  final val CallAlreadyClosed        = "call already closed"
  final val WrongMessageType: String = "Wrong Message Type"
  final val EmptyToken: String       = "Empty Token provided"
  final val AuthInfoNotValid         = "Auth Info Not Valid"

  sealed trait AuthInfo
  private case class DbAuthInto private (db: String)                                        extends AuthInfo
  private case class NamespaceAuthInto private (db: String, namespace: String)              extends AuthInfo
  private case class MetricAuthInto private (db: String, namespace: String, metric: String) extends AuthInfo

  object AuthInfo {
    def validate(map: Map[AuthorizationLevel, String]): Either[String, AuthInfo] = {
      (map.get(AuthorizationLevel.DB), map.get(AuthorizationLevel.NAMESPACE), map.get(AuthorizationLevel.METRIC)) match {
        case (Some(db), None, None) =>
          Right(DbAuthInto(db))
        case (Some(db), Some(namespace), None) =>
          Right(NamespaceAuthInto(db, namespace))
        case (Some(db), Some(namespace), Some(metric)) =>
          Right(MetricAuthInto(db, namespace, metric))
        case _ =>
          Left(AuthInfoNotValid)
      }
    }
  }

}
