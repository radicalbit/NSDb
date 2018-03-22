package io.radicalbit.nsdb.web

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.InputMapped
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}
import io.radicalbit.nsdb.web.Formats._
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes.{DataApi, InsertBody}
import org.json4s._
import org.scalatest._

import scala.concurrent.duration._

class FakeWriteCoordinator extends Actor {
  override def receive: Receive = {
    case msg: MapInput => sender() ! InputMapped(msg.db, msg.namespace, msg.metric, msg.record)
  }
}

class DataApiTest extends FlatSpec with Matchers with ScalatestRouteTest {

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

    Post("/data", b).withHeaders(RawHeader("wrong", "wrong")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized header not provided"
    }

  }

  "Secured DataApi" should "not allow a request for an unauthorized resources" in {
    val b = InsertBody("db", "namespace", "notAuthorizedMetric", Bit(0, 1, Map.empty))

    Post("/data", b).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden access to metric notAuthorizedMetric"
    }

    val readOnly = InsertBody("db", "namespace", "readOnlyMetric", Bit(0, 1, Map.empty))

    Post("/data", readOnly).withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe Forbidden
      entityAs[String] shouldBe "not authorized forbidden write access to metric readOnlyMetric"
    }
  }

  "Secured DataApi" should "allow a request for an authorized resources" in {
    val b = InsertBody("db", "namespace", "metric", Bit(0, 1, Map.empty))

    Post("/data", b).withHeaders(RawHeader("testHeader", "testHeader")) ~> testRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe "OK"
    }
  }
}
