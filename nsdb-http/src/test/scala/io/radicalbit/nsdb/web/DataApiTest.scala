package io.radicalbit.nsdb.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.MapInput
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.InputMapped
import io.radicalbit.nsdb.web.Formats._
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
    apiResources(null, null, system.actorOf(Props[FakeWriteCoordinator]))
  )

  "DataApi" should "not allow get" in {
    Get("/data/db") ~> testRoutes ~> check {
      status shouldEqual MethodNotAllowed
    }
  }

  "DataApi" should "correctly insert a record" in {
    val b = InsertBody("namespace", "metric", Bit(0, 1, Map.empty))

    Post("/data/db", b) ~> testRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      println(entity)
      entity shouldBe "OK"
    }
  }
}
