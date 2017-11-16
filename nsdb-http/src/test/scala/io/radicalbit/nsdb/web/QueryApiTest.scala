package io.radicalbit.nsdb.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.RangeExpression
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementExecuted
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
      sender ! SelectStatementExecuted(statement.namespace, statement.metric, bitsParameterized(e.value1, e.value2))
    case ExecuteStatement(statement) => sender ! SelectStatementExecuted(statement.namespace, statement.metric, bits)
  }
}

object FakeReadCoordinator {
  def bitsParameterized(from: Long, to: Long) =
    Seq(Bit(from, 1, Map("name" -> "name", "number" -> 2)), Bit(to, 3, Map("name" -> "name", "number" -> 2)))
  val bits = Seq(Bit(0, 1, Map("name" -> "name", "number" -> 2)), Bit(2, 3, Map("name" -> "name", "number" -> 2)))
}

class QueryApiTest extends FlatSpec with Matchers with ScalatestRouteTest with QueryResources {

  implicit val formats          = DefaultFormats
  implicit val timeout: Timeout = 5 seconds

  val testRoutes = Route.seal(
    queryResources(null, system.actorOf(Props[FakeReadCoordinator]))
  )

  "QueryApi" should "not allow get" in {
    Get("/query") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

  "QueryApi" should "correctly query the db with time range passed" in {
    val q = QueryBody("namespace", "select * from metric limit 1", Some(100), Some(200))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe OK
      val entity       = entityAs[String]
      val recordString = pretty(render((parse(entity))))

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
    val q = QueryBody("namespace", "select * from metric limit 1", None, None)

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

  "QueryApi" should "return if query is not valid" in {
    val q = QueryBody("namespace", "select from metric", Some(1), Some(2))

    Post("/query", q) ~> testRoutes ~> check {
      status shouldBe BadRequest

    }

  }
}
