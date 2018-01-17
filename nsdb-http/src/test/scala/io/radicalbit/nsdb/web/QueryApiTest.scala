package io.radicalbit.nsdb.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.RangeExpression
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementExecuted
import io.radicalbit.nsdb.security.http.EmptyAuthorization
import io.radicalbit.nsdb.web.Formats._
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest._

import scala.concurrent.duration._

class FakeReadCoordinator extends Actor {
  import FakeReadCoordinator._
  override def receive: Receive = {
    case ExecuteStatement(statement)
        if statement.condition.isDefined && statement.condition.get.expression.isInstanceOf[RangeExpression[Long]] =>
      val e = statement.condition.get.expression.asInstanceOf[RangeExpression[Long]]
      sender ! SelectStatementExecuted(statement.db,
                                       statement.namespace,
                                       statement.metric,
                                       bitsParametrized(e.value1, e.value2))
    case ExecuteStatement(statement) =>
      sender ! SelectStatementExecuted(statement.db, statement.namespace, statement.metric, bits)
  }
}

object FakeReadCoordinator {
  def bitsParametrized(from: Long, to: Long) =
    Seq(Bit(from, 1, Map("name" -> "name", "number" -> 2)), Bit(to, 3, Map("name" -> "name", "number" -> 2)))
  val bits = Seq(Bit(0, 1, Map("name" -> "name", "number" -> 2)), Bit(2, 3, Map("name" -> "name", "number" -> 2)))
}

class QueryApiTest extends FlatSpec with Matchers with ScalatestRouteTest with ApiResources {

  implicit val formats          = DefaultFormats
  implicit val timeout: Timeout = 5 seconds

  val testRoutes = Route.seal(
    apiResources(null, system.actorOf(Props[FakeReadCoordinator]), null, new EmptyAuthorization)
  )

  val testSecuredRoutes = Route.seal(
    apiResources(null, system.actorOf(Props[FakeReadCoordinator]), null, new TestAuthProvider)
  )

  "QueryApi" should "not allow get" in {
    Get("/query") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

  "QueryApi" should "correctly query the db with time range passed" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1", Some(100), Some(200))

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
        |    }
        |  }, {
        |    "timestamp" : 200,
        |    "value" : 3,
        |    "dimensions" : {
        |      "name" : "name",
        |      "number" : 2
        |    }
        |  } ]
        |}""".stripMargin

    }

  }

  "QueryApi" should "correctly query the db without time range passed" in {
    val q = QueryBody("db", "namespace", "metric", "select * from metric limit 1", None, None)

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render((parse(entity))))

      recordString shouldBe
        """{
          |  "records" : [ {
          |    "timestamp" : 0,
          |    "value" : 1,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    }
          |  }, {
          |    "timestamp" : 2,
          |    "value" : 3,
          |    "dimensions" : {
          |      "name" : "name",
          |      "number" : 2
          |    }
          |  } ]
          |}""".stripMargin

    }
  }

  "Secured QueryApi" should "not allow a request without the security header" in {
    val q = QueryBody("db", "namespace", "metric", "select from metric", Some(1), Some(2))

    Post("/query", q) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
        entityAs[String] shouldBe "not authorized header not provided"
    }

    Post("/query", q).withHeaders(RawHeader("wrong","wrong")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

  }

  "Secured QueryApi" should "not allow a request for an unauthorized resources" in {
    val q = QueryBody("db", "namespace", "notAuthorizedMetric", "select from metric", Some(1), Some(2))

    Post("/query", q).withHeaders(RawHeader("testHeader","testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden access to db notAuthorizedMetric"
    }
  }

  "Secured QueryApi" should "allow a request for an authorized resources" in {
    val q = QueryBody("db", "namespace", "metric", "select from metric", Some(1), Some(2))

    Post("/query", q).withHeaders(RawHeader("testHeader","testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }
}
