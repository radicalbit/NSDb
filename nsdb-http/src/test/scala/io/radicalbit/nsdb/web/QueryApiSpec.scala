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
import io.radicalbit.nsdb.common.NSDbLongType
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}
import io.radicalbit.nsdb.web.NSDbJsonProtocol._
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class QueryApiSpec extends FlatSpec with Matchers with ScalatestRouteTest {

  val readCoordinatorActor: ActorRef = system.actorOf(Props[FakeReadCoordinator])

  val secureAuthenticationProvider: NSDBAuthProvider  = new TestAuthProvider
  val emptyAuthenticationProvider: EmptyAuthorization = new EmptyAuthorization

  /*
        adds to formats a CustomSerializerForTest that serializes relative timestamp (now) with a fake
        fixed timestamp (0L) in order to make the unit test time-independent
   */
  implicit val formats
    : Formats = DefaultFormats ++ CustomSerializers.customSerializers + CustomSerializerForTest + BitSerializer

  val secureQueryApi = new QueryApi {
    override def authenticationProvider: NSDBAuthProvider = secureAuthenticationProvider

    override def readCoordinator: ActorRef = readCoordinatorActor
    override implicit val timeout: Timeout = 5 seconds

  }

  val emptyQueryApi = new QueryApi {
    override def authenticationProvider: NSDBAuthProvider = emptyAuthenticationProvider

    override def readCoordinator: ActorRef = readCoordinatorActor
    override implicit val timeout: Timeout = 5 seconds
  }

  val testRoutes = Route.seal(
    emptyQueryApi.queryApi()(system.log, formats)
  )

  val testSecuredRoutes = Route.seal(
    secureQueryApi.queryApi()(system.log, formats)
  )

  "QueryApi" should "not allow get" in {
    Get("/query") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

  "QueryApi" should "correctly query the db with a single filter over Long" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric limit 1",
                None,
                None,
                Some(Seq(FilterByValue("value", 1L, FilterOperators.Equality))),
                None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ]
          |}""".stripMargin

    }

  }

  "QueryApi" should "correctly query the db with a single filter and with time range" in {
    val q =
      QueryBody(
        "db",
        "namespace",
        "metric",
        "select * from metric limit 1",
        Some(NSDbLongType(100)),
        Some(NSDbLongType(200)),
        Some(Seq(FilterByValue("value", 1L, FilterOperators.Equality))),
        None
      )

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ]
          |}""".stripMargin

    }

  }

  "QueryApi" should "correctly query the db with a single filter with compatible type" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric limit 1",
                None,
                None,
                Some(Seq(FilterByValue("value", "1", FilterOperators.Equality))),
                None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
    }

  }

  "QueryApi" should "fail with a single filter with incompatible type" in {
    val q =
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric limit 1",
                None,
                None,
                Some(Seq(FilterByValue("value", "vd", FilterOperators.Equality))),
                None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe InternalServerError
    }
  }

  "QueryApi" should "correctly query the db with time range passed" in {
    val q = QueryBody("db",
                      "namespace",
                      "metric",
                      "select * from metric limit 1",
                      Some(NSDbLongType(100)),
                      Some(NSDbLongType(200)),
                      None,
                      None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
        |  "records" : [ {
        |    "timestamp" : 100,
        |    "value" : 1,
        |    "dimensions" : {
        |      "name" : "name",
        |      "number" : 2
        |    },
        |    "tags" : {
        |      "country" : "country"
        |    }
        |  }, {
        |    "timestamp" : 200,
        |    "value" : 3,
        |    "dimensions" : {
        |      "name" : "name",
        |      "number" : 2
        |    },
        |    "tags" : {
        |      "country" : "country"
        |    }
        |  } ]
        |}""".stripMargin

    }

  }

  "QueryApi" should "correctly query the db" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1")

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render(parse(entity)))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    },
          |    "tags" : {
          |      "country" : "country"
          |    }
          |  } ]
          |}""".stripMargin

    }
  }

  "Secured QueryApi" should "not allow a request without the security header" in {
    val q = QueryBody("db",
                      "namespace",
                      "metric",
                      "select from metric",
                      Some(NSDbLongType(1)),
                      Some(NSDbLongType(2)),
                      None,
                      None)

    Post("/query", q) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

    Post("/query", q).withHeaders(RawHeader("wrong", "wrong")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

  }

  "Secured QueryApi" should "not allow a request for an unauthorized resources" in {
    val q = QueryBody("db",
                      "namespace",
                      "notAuthorizedMetric",
                      "select from metric",
                      Some(NSDbLongType(1)),
                      Some(NSDbLongType(2)),
                      None,
                      None)

    Post("/query", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden access to metric notAuthorizedMetric"
    }
  }

  "Secured QueryApi" should "allow a request for an authorized resources" in {
    val q = QueryBody("db",
                      "namespace",
                      "metric",
                      "select * from metric limit 1",
                      Some(NSDbLongType(1)),
                      Some(NSDbLongType(2)),
                      None,
                      None)

    Post("/query", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }
}
