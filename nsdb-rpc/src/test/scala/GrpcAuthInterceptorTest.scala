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

import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{ManagedChannel, Server, ServerInterceptors, Status, StatusRuntimeException}
import io.radicalbit.nsdb.client.rpc.TokenAppliers
import io.radicalbit.nsdb.rpc.server.GrpcAuthInterceptor
import io.radicalbit.nsdb.rpc.test.DummyServiceGrpc.DummySecureServiceGrpc
import io.radicalbit.nsdb.rpc.test.securityTest.{DummyResponse, DummySecureRequest, DummySecureServiceGrpc}
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider.AuthorizationResponse
import io.radicalbit.nsdb.test.NSDbSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext

class GrpcAuthInterceptorTest extends NSDbSpec with BeforeAndAfterAll with BeforeAndAfterEach with ScalaFutures {

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  val channelName: String           = InProcessServerBuilder.generateName
  var clientChannel: ManagedChannel = _
  var server: Server                = _

  val testAuthProvider: NSDbAuthorizationProvider = new NSDbAuthorizationProvider {
    override def extractHttpSecurityPayload(rawHeaders: util.Map[String, String]): String = ""

    override def extractWsSecurityPayload(subProtocols: util.List[String]): String = ""

    override def getGrpcSecurityHeader: String = "Authorization"

    override def checkDbAuth(db: String,
                             payload: String,
                             writePermission: Boolean): NSDbAuthorizationProvider.AuthorizationResponse =
      if (payload == "Bearer allowed" && db == "allowedDb") new AuthorizationResponse(true)
      else new AuthorizationResponse(false, "Not Allowed")

    override def checkNamespaceAuth(db: String,
                                    namespace: String,
                                    payload: String,
                                    writePermission: Boolean): NSDbAuthorizationProvider.AuthorizationResponse =
      if (payload == "Bearer allowed" && db == "allowedDb" && namespace == "allowedNamespace")
        new AuthorizationResponse(true)
      else new AuthorizationResponse(false, "Not Allowed")

    override def checkMetricAuth(db: String,
                                 namespace: String,
                                 metric: String,
                                 payload: String,
                                 writePermission: Boolean): NSDbAuthorizationProvider.AuthorizationResponse =
      if (payload == "Bearer allowed" && db == "allowedDb" && namespace == "allowedNamespace" && metric == "allowedMetric")
        new AuthorizationResponse(true)
      else new AuthorizationResponse(false, "Not Allowed")
  }

  val grpcAuthProvider = new GrpcAuthInterceptor(testAuthProvider)

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(1, Seconds), interval = Span(500, Millis))

  override def beforeAll: Unit = {
    val dummySecureService = new DummySecureServiceGrpc
    server = InProcessServerBuilder
      .forName(channelName)
      .addService(
        ServerInterceptors.intercept(DummySecureServiceGrpc.bindService(dummySecureService, ec), grpcAuthProvider))
      .build
      .start
  }

  override def beforeEach(): Unit =
    clientChannel = InProcessChannelBuilder.forName(channelName).usePlaintext.build

  override def afterEach(): Unit = {
    Option(clientChannel).foreach(_.shutdownNow())
    Option(clientChannel).foreach(_.awaitTermination(1, TimeUnit.SECONDS))
  }

  override def afterAll(): Unit =
    Option(server).foreach(_.shutdownNow())
  Option(server).foreach(_.awaitTermination())

  "GrpcAuthInterceptor" should {
    "Deny a call without a token provided" in {
      val dummyServiceSecureClient = DummySecureServiceGrpc.stub(clientChannel)

      whenReady(dummyServiceSecureClient.dummySecure(DummySecureRequest("db", "namespace", "metric")).failed) {
        throwable =>
          throwable shouldBe an[StatusRuntimeException]
          throwable.asInstanceOf[StatusRuntimeException].getStatus.getCode shouldBe Status.UNAUTHENTICATED.getCode
          throwable
            .asInstanceOf[StatusRuntimeException]
            .getStatus
            .getDescription shouldBe GrpcAuthInterceptor.EmptyToken
      }
    }
  }

  "Deny a call with a wrong token provided" in {
    val dummyServiceSecureClient =
      DummySecureServiceGrpc.stub(clientChannel).withCallCredentials(TokenAppliers.JWT("wrongToken"))

    whenReady(dummyServiceSecureClient.dummySecure(DummySecureRequest("db", "namespace", "metric")).failed) {
      throwable =>
        throwable shouldBe an[StatusRuntimeException]
        throwable.asInstanceOf[StatusRuntimeException].getStatus.getCode shouldBe Status.PERMISSION_DENIED.getCode
        throwable.asInstanceOf[StatusRuntimeException].getStatus.getDescription shouldBe "Not Allowed"
    }
  }

  "Deny a call with the right token but not authorized command" in {
    val dummyServiceSecureClient =
      DummySecureServiceGrpc.stub(clientChannel).withCallCredentials(TokenAppliers.JWT("allowed"))

    whenReady(dummyServiceSecureClient.dummySecure(DummySecureRequest("db", "namespace", "metric")).failed) {
      throwable =>
        throwable shouldBe an[StatusRuntimeException]
        throwable.asInstanceOf[StatusRuntimeException].getStatus.getCode shouldBe Status.PERMISSION_DENIED.getCode
        throwable
          .asInstanceOf[StatusRuntimeException]
          .getStatus
          .getDescription shouldBe "Not Allowed"
    }
  }

  "Allow a call with the right token and an authorized command" in {
    val dummyServiceSecureClient =
      DummySecureServiceGrpc.stub(clientChannel).withCallCredentials(TokenAppliers.JWT("allowed"))

    whenReady(
      dummyServiceSecureClient.dummySecure(DummySecureRequest("allowedDb", "allowedNamespace", "allowedMetric"))) {
      response =>
        response shouldBe DummyResponse("allowedDb-allowedNamespace-allowedMetric")
    }
  }
}
