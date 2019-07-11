/*
 * Copyright 2018 Radicalbit S.r.l.
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
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.RangeExpression
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteStatement, MapInput}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{InputMapped, SelectStatementExecuted, SelectStatementFailed}
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.web.Formats._
import io.radicalbit.nsdb.web.QueryApiTest.{FakeReadCoordinator, FakeWriteCoordinator}
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.util.Failure

object QueryApiTest {
  class FakeReadCoordinator extends Actor {
    import FakeReadCoordinator._
    override def receive: Receive = {
      case ExecuteStatement(statement)
          if statement.condition.isDefined && statement.condition.get.expression.isInstanceOf[RangeExpression[Long]] =>
        StatementParser.parseStatement(
          statement,
          Schema("metric", bits.head).getOrElse(Schema("metric", Set.empty[SchemaField]))) match {
          case scala.util.Success(_) =>
            val e = statement.condition.get.expression.asInstanceOf[RangeExpression[Long]]
            sender ! SelectStatementExecuted(statement.db,
                                             statement.namespace,
                                             statement.metric,
                                             bitsParametrized(e.value1, e.value2))
          case Failure(_) => sender ! SelectStatementFailed("statement not valid")
        }
      case ExecuteStatement(statement) =>
        StatementParser.parseStatement(
          statement,
          Schema("metric", bits.head).getOrElse(Schema("metric", Set.empty[SchemaField]))) match {
          case scala.util.Success(_) =>
            sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
          case Failure(_) => sender ! SelectStatementFailed("statement not valid")
        }
    }
  }

  object FakeReadCoordinator {
    def bitsParametrized(from: Long, to: Long) =
      Seq(Bit(from, 1, Map("name" -> "name", "number" -> 2), Map("country" -> "country")),
          Bit(to, 3, Map("name"   -> "name", "number" -> 2), Map("country" -> "country")))
    val bits = Seq(Bit(0, 1, Map("name" -> "name", "number" -> 2), Map("country" -> "country")),
                   Bit(2, 3, Map("name" -> "name", "number" -> 2), Map("country" -> "country")))
  }

  class FakeWriteCoordinator extends Actor {
    override def receive: Receive = {
      case msg: MapInput => sender() ! InputMapped(msg.db, msg.namespace, msg.metric, msg.record)
    }
  }
}

class QueryApiTest extends FlatSpec with Matchers with ScalatestRouteTest {

  val writeCoordinatorActor: ActorRef = system.actorOf(Props[FakeWriteCoordinator])
  val readCoordinatorActor: ActorRef  = system.actorOf(Props[FakeReadCoordinator])

  val secureAuthenticationProvider: NSDBAuthProvider  = new TestAuthProvider
  val emptyAuthenticationProvider: EmptyAuthorization = new EmptyAuthorization

  val secureQueryApi = new QueryApi {
    override def authenticationProvider: NSDBAuthProvider = secureAuthenticationProvider

    override def writeCoordinator: ActorRef       = writeCoordinatorActor
    override def readCoordinator: ActorRef        = readCoordinatorActor
    override def publisherActor: ActorRef         = null
    override implicit val formats: DefaultFormats = DefaultFormats
    override implicit val timeout: Timeout        = 5 seconds

  }

  val emptyQueryApi = new QueryApi {
    override def authenticationProvider: NSDBAuthProvider = emptyAuthenticationProvider

    override def writeCoordinator: ActorRef = writeCoordinatorActor
    override def readCoordinator: ActorRef  = readCoordinatorActor
    override def publisherActor: ActorRef   = null

    override implicit val formats: DefaultFormats = DefaultFormats
    override implicit val timeout: Timeout        = 5 seconds
  }

  val testRoutes = Route.seal(
    emptyQueryApi.queryApi()(system.log)
  )

  val testSecuredRoutes = Route.seal(
    secureQueryApi.queryApi()(system.log)
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
                Some(Seq(FilterByValue("value", 1L, FilterOperators.Equality))))

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
      QueryBody("db",
                "namespace",
                "metric",
                "select * from metric limit 1",
                Some(100),
                Some(200),
                Some(Seq(FilterByValue("value", 1L, FilterOperators.Equality))))

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
                Some(Seq(FilterByValue("value", "1", FilterOperators.Equality))))

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
                Some(Seq(FilterByValue("value", "vd", FilterOperators.Equality))))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe InternalServerError
    }
  }

  "QueryApi" should "correctly query the db with time range passed" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1", Some(100), Some(200), None)

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

  "QueryApi" should "correctly query the db without time range passed" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1", None, None, None)

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
    val q = QueryBody("db", "namespace", "metric", "select from metric", Some(1), Some(2), None)

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
    val q = QueryBody("db", "namespace", "notAuthorizedMetric", "select from metric", Some(1), Some(2), None)

    Post("/query", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden access to metric notAuthorizedMetric"
    }
  }

  "Secured QueryApi" should "allow a request for an authorized resources" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1", Some(1), Some(2), None)

    Post("/query", q).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }
}
