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

package io.radicalbit.nsdb.web

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.{RejectionHandler, Route}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.InputMapped
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}
import io.radicalbit.nsdb.test.NSDbFlatSpec
import io.radicalbit.nsdb.web.DataApiTest.FakeWriteCoordinator
import io.radicalbit.nsdb.web.NSDbJson._
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes.{DataApi, InsertBody}
import io.radicalbit.nsdb.web.validation.{FieldErrorInfo, ModelValidationRejection}
import org.json4s._
import org.json4s.jackson.Serialization.write

import scala.concurrent.duration._

object DataApiTest {
  class FakeWriteCoordinator extends Actor {
    override def receive: Receive = {
      case msg: MapInput => sender() ! InputMapped(msg.db, msg.namespace, msg.metric, msg.record)
    }
  }
}

trait MyRejectionHandler {
  import org.json4s.jackson.Serialization.write
  implicit val formats = DefaultFormats

  implicit def myRejectionHandler: RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handle {
        case ModelValidationRejection(data) =>
          complete(HttpResponse(BadRequest, entity = write(data)))
      }
      .result()
}

class DataApiTest extends NSDbFlatSpec with ScalatestRouteTest with MyRejectionHandler {

  val writeCoordinatorActor: ActorRef = system.actorOf(Props[FakeWriteCoordinator])

  val secureAuthenticationProvider: NSDBAuthProvider  = new TestAuthProvider
  val emptyAuthenticationProvider: EmptyAuthorization = new EmptyAuthorization

  val secureDataApi = new DataApi {
    override def authenticationProvider: NSDBAuthProvider = secureAuthenticationProvider

    override def writeCoordinator: ActorRef       = writeCoordinatorActor
    override implicit val formats: DefaultFormats = DefaultFormats
    override implicit val timeout: Timeout        = 5 seconds
  }

  val emptyDataApi = new DataApi {
    override def authenticationProvider: NSDBAuthProvider = emptyAuthenticationProvider

    override def writeCoordinator: ActorRef = writeCoordinatorActor

    override implicit val formats: DefaultFormats = DefaultFormats
    override implicit val timeout: Timeout        = 5 seconds
  }

  val testRoutes = Route.seal(
    emptyDataApi.dataApi
  )

  val testSecuredRoutes = Route.seal(
    secureDataApi.dataApi
  )

  "DataApi" should "not allow get" in {
    Get("/data") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

  "DataApi" should "correctly insert a record" in {
    val b = InsertBody("db", "namespace", "metric", Bit(0, 1, Map.empty, Map.empty))

    Post("/data", b) ~> testRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe "OK"
    }
  }

  "DataApi" should "validate the InsertBody" in {
    val b = InsertBody("db", "namespace", "ghost-metric", Bit(0, 1, Map.empty, Map.empty))
    Post("/data", b) ~> testRoutes ~> check {
      status shouldBe BadRequest
      val entity = entityAs[String]
      entity shouldBe write(Seq(FieldErrorInfo("metric", "Field metric must contain only alphanumeric chars")))
    }
  }

  "Secured DataApi" should "not allow a request without the security header" in {
    val b = InsertBody("db", "namespace", "metric", Bit(0, 1, Map.empty, Map.empty))

    Post("/data", b) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

    Post("/data", b).withHeaders(RawHeader("wrong", "wrong")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

  }

  "Secured DataApi" should "not allow a request for an unauthorized resources" in {
    val b = InsertBody("db", "namespace", "notAuthorizedMetric", Bit(0, 1, Map.empty, Map.empty))

    Post("/data", b).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden access to metric notAuthorizedMetric"
    }

    val readOnly = InsertBody("db", "namespace", "readOnlyMetric", Bit(0, 1, Map.empty, Map.empty))

    Post("/data", readOnly).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden write access to metric readOnlyMetric"
    }
  }

  "Secured DataApi" should "allow a request for an authorized resources" in {
    val b = InsertBody("db", "namespace", "metric", Bit(0, 1, Map.empty, Map.empty))

    Post("/data", b).withHeaders(RawHeader("testHeader", "testHeader")) ~> testRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe "OK"
    }
  }
}
