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

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.actor.FakeReadCoordinator
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}
import io.radicalbit.nsdb.web.NSDbJson._
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes._
import org.json4s._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class QueryValidationApiSpec extends FlatSpec with Matchers with ScalatestRouteTest {

  val readCoordinatorActor: ActorRef = system.actorOf(Props[FakeReadCoordinator])

  val secureAuthenticationProvider: NSDBAuthProvider  = new TestAuthProvider
  val emptyAuthenticationProvider: EmptyAuthorization = new EmptyAuthorization

  val secureQueryValidationApi = new QueryValidationApi {
    override def authenticationProvider: NSDBAuthProvider = secureAuthenticationProvider

    override def readCoordinator: ActorRef        = readCoordinatorActor
    override implicit val formats: DefaultFormats = DefaultFormats
    override implicit val timeout: Timeout        = 5 seconds

  }

  val emptyQueryVAlidationApi = new QueryValidationApi {
    override def authenticationProvider: NSDBAuthProvider = emptyAuthenticationProvider

    override def readCoordinator: ActorRef = readCoordinatorActor

    override implicit val formats: DefaultFormats = DefaultFormats
    override implicit val timeout: Timeout        = 5 seconds
  }

  val testRoutes = Route.seal(
    emptyQueryVAlidationApi.queryValidationApi(system.log)
  )

  val testSecuredRoutes = Route.seal(
    secureQueryValidationApi.queryValidationApi(system.log)
  )

  "QueryValidationApi" should "not allow get" in {
    Get("/query/validate") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

  "QueryValidationApi" should "correctly validate a correct query" in {
    val q =
      QueryValidationBody("db", "namespace", "metric1", "select * from metric1 limit 1")

    Post("/query/validate", q) ~> testRoutes ~> check {
      status shouldBe OK
    }
  }

  "QueryValidationApi" should "give validation error against a malformed sql" in {
    val q =
      QueryValidationBody("db", "namespace", "metric", "select")

    Post("/query/validate", q) ~> testRoutes ~> check {
      status shouldBe BadRequest
    }
  }

  "QueryValidationApi" should "give not found against a query for a non existing metric" in {
    val q =
      QueryValidationBody("db", "namespace", "nonExisting", "select * from nonExisting limit 1")

    Post("/query/validate", q) ~> testRoutes ~> check {
      status shouldBe NotFound
    }
  }

  "Secured QueryValidationApi" should "not allow a request without the security header" in {
    val q = QueryValidationBody("db", "namespace", "metric", "select from metric1")

    Post("/query/validate", q) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

    Post("/query/validate", q).withHeaders(RawHeader("wrong", "wrong")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

  }

  "Secured QueryValidationApi" should "not allow a request for an unauthorized resources" in {
    val q = QueryValidationBody("db", "namespace", "notAuthorizedMetric", "select from metric1")

    Post("/query/validate", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden access to metric notAuthorizedMetric"
    }
  }

  "Secured QueryValidationApi" should "allow a request for an authorized resources" in {
    val q = QueryValidationBody("db", "namespace", "metric", "select * from metric1 limit 1")

    Post("/query/validate", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }
}
