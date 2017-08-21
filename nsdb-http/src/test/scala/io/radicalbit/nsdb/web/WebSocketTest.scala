package io.radicalbit.nsdb.web

import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import io.radicalbit.nsdb.web.actor.StreamActor.{OutgoingMessage, QueryRegistrationFailed}
import org.scalatest.{FlatSpec, Matchers}
import org.json4s._
import org.json4s.native.JsonMethods._

class WebSocketTest() extends FlatSpec with ScalatestRouteTest with Matchers with WsResources {

  val wsClient = WSProbe()

  "WebSocketStream" should "register to a query" in {
    WS("/ws-stream", wsClient.flow) ~> wsResources(null) ~>
      check {
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage("Invalid Message")
        wsClient.expectMessage("""{"message":"invalid message sent"}""")

        wsClient.sendMessage("""{"namespace":"a","queryString":"INSERT INTO people DIM(name=john) FLD(value=23)"}""")

        """{"namespace":"registry","queryString":"select * from people limit 1"}"""

        val text = wsClient.expectMessage().asTextMessage.getStrictText

        val obj: Option[OutgoingMessage] = parse(text).extractOpt[OutgoingMessage]

        obj.isDefined shouldBe true
        obj.get.message.asInstanceOf[Map[String, String]].get("reason") shouldBe Some("not a select query")
      }
  }
}
