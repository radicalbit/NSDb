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
import io.radicalbit.nsdb.actor.{FakeWriteCoordinator, FakeReadCoordinator}
import io.radicalbit.nsdb.common.NSDbLongType
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}
import io.radicalbit.nsdb.web.Filters._
import io.radicalbit.nsdb.web.NSDbJson._
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import io.radicalbit.nsdb.test.NSDbSpec

import scala.concurrent.duration._

class QueryApiSpec extends NSDbSpec with ScalatestRouteTest {

  val readCoordinatorActor: ActorRef  = system.actorOf(Props[FakeReadCoordinator])
  val writeCoordinatorActor: ActorRef = system.actorOf(Props[FakeWriteCoordinator])

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

    override def readCoordinator: ActorRef  = readCoordinatorActor
    override def writeCoordinator: ActorRef = writeCoordinatorActor
    override implicit val timeout: Timeout  = 5 seconds

  }

  val emptyQueryApi = new QueryApi {
    override def authenticationProvider: NSDBAuthProvider = emptyAuthenticationProvider

    override def readCoordinator: ActorRef  = readCoordinatorActor
    override def writeCoordinator: ActorRef = writeCoordinatorActor
    override implicit val timeout: Timeout  = 5 seconds
  }

  val testRoutes = Route.seal(
    emptyQueryApi.queryApi()(system.log, formats)
  )

  val testSecuredRoutes = Route.seal(
    secureQueryApi.queryApi()(system.log, formats)
  )

  "QueryApi for select statements" should {
    "correctly query the db using get verb" in {
      val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1")

      Get("/query", q) ~> testRoutes ~> check {
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

    "correctly query the db with a single filter over Long" in {
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

    "correctly query the db with a single filter and with time range" in {
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

    "correctly query the db with a single filter with compatible type" in {
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

    "fail with a single filter with incompatible type" in {
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

    "correctly query the db with time range passed" in {
      val q = QueryBody("db",
                        "namespace",
                        "metric",
                        "select * from metric limit 1",
                        Some(NSDbLongType(1L)),
                        Some(NSDbLongType(2L)),
                        None,
                        None)

      Post("/query", q) ~> testRoutes ~> check {
        status shouldBe OK
        val entity       = entityAs[String]
        val recordString = pretty(render(parse(entity)))

        recordString shouldBe
          """{
            |  "records" : [ {
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

    "correctly query the db" in {
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
  }

  "QueryApi for delete statements" should {

    "not work in GET" in {
      val q = QueryBody("db", "namespace", "metric", "delete from metric where timestamp > 0")

      Get("/query", q) ~> testRoutes ~> check {
        status shouldBe MethodNotAllowed
      }
    }

    "not support invalid statements" in {
      val q = QueryBody("db", "namespace", "metric", "delete from metric")

      Post("/query", q) ~> testRoutes ~> check {
        status shouldBe BadRequest
      }
    }

    "supports valid delete statements" in {
      val q = QueryBody("db", "namespace", "metric", "delete from metric where timestamp > 0")

      Post("/query", q) ~> testRoutes ~> check {
        status shouldBe OK

        entityAs[String] shouldBe "Ok"
      }
    }

    "properly handle delete error messages" in {
      Post("/query",
           QueryBody(
             "db",
             "namespace",
             "metric",
             s"delete from ${FakeWriteCoordinator.notExistingMetric} where timestamp > 0")) ~> testRoutes ~> check {
        status shouldBe NotFound
      }

      Post("/query",
           QueryBody("db",
                     "namespace",
                     "metric",
                     s"delete from ${FakeWriteCoordinator.errorMetric} where timestamp > 0")) ~> testRoutes ~> check {
        status shouldBe InternalServerError
      }
    }
  }

  "Secured QueryApi" should {
    "not allow a request without the security header" in {
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

    "not allow a request for an unauthorized resources" in {
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

    "allow a request for an authorized resources" in {
      val q = QueryBody("db",
                        "namespace",
                        "metric",
                        "select * from metric limit 1",
                        Some(NSDbLongType(1L)),
                        Some(NSDbLongType(2L)),
                        None,
                        None)

      Post("/query", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
        status shouldBe OK
      }
    }
  }
}
