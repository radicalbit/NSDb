package io.radicalbit.nsdb.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.InputMapped
import io.radicalbit.nsdb.security.http.EmptyAuthorization
import io.radicalbit.nsdb.web.Formats._
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import org.json4s._
import org.scalatest._

import scala.concurrent.duration._

class FakeWriteCoordinator extends Actor {
  override def receive: Receive = {
    case msg: MapInput => sender() ! InputMapped(msg.db, msg.namespace, msg.metric, msg.record)
  }
}

class DataApiTest extends FlatSpec with Matchers with ScalatestRouteTest with ApiResources {

  implicit val formats          = DefaultFormats
  implicit val timeout: Timeout = 5 seconds

  val testRoutes = Route.seal(
    apiResources(null, null, system.actorOf(Props[FakeWriteCoordinator]), new EmptyAuthorization)
  )

  val testSecuredRoutes = Route.seal(
    apiResources(null, null, system.actorOf(Props[FakeWriteCoordinator]), new TestAuthProvider)
  )

  "DataApi" should "not allow get" in {
    Get("/data") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

  "DataApi" should "correctly insert a record" in {
    val b = InsertBody("db", "namespace", "metric", Bit(0, 1, Map.empty))

    Post("/data", b) ~> testRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe "OK"
    }
  }

  "Secured DataApi" should "not allow a request without the security header" in {
    val b = InsertBody("db", "namespace", "metric", Bit(0, 1, Map.empty))

    Post("/data", b) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

    Post("/data", b).withHeaders(RawHeader("wrong","wrong")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

  }

  "Secured DataApi" should "not allow a request for an unauthorized resources" in {
    val b = InsertBody("db", "namespace", "notAuthorizedMetric", Bit(0, 1, Map.empty))

    Post("/data", b).withHeaders(RawHeader("testHeader","testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden access to db notAuthorizedMetric"
    }
  }

  "Secured DataApi" should "allow a request for an authorized resources" in {
    val b = InsertBody("db", "namespace", "metric", Bit(0, 1, Map.empty))

    Post("/data", b).withHeaders(RawHeader("testHeader","testHeader")) ~> testRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe "OK"
    }
  }
}
