package io.radicalbit.nsdb.web

import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.PublisherActor.Events.Subscribed
import io.radicalbit.nsdb.web.actor.StreamActor.QuerystringRegistrationFailed
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

class WebSocketTest() extends FlatSpec with ScalatestRouteTest with Matchers with WsResources {

  val basePath       = "target/test_index_ws"
  val publisherActor = system.actorOf(PublisherActor.props(basePath))

  val wsClient = WSProbe()

  "WebSocketStream" should "register to a query" in {
    WS("/ws-stream", wsClient.flow) ~> wsResources(publisherActor) ~>
      check {
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage("""{"namespace":"a","queryString":"INSERT INTO people DIM(name=john) FLD(value=23)"}""")

        val text = wsClient.expectMessage().asTextMessage.getStrictText

        val obj: Option[QuerystringRegistrationFailed] = parse(text).extractOpt[QuerystringRegistrationFailed]

        obj.isDefined shouldBe true
        obj.get.reason shouldEqual "not a select query"

        wsClient.sendMessage("""{"namespace":"registry","queryString":"select * from people limit 1"}""")

        val subscribed = wsClient.expectMessage().asTextMessage.getStrictText
        parse(subscribed).extractOpt[Subscribed].isDefined shouldBe true

        //TODO find out how to test it, i.e. combine somehow the actorsystem coming from ScalatestRouteTest and from Testkit
//        publisherActor ! RecordPublished("metric", Record(0, Map("dim1" -> "v1"), Map.empty))
//
//        val published = wsClient.expectMessage().asTextMessage.getStrictText
//        parse(published).extractOpt[RecordPublished].isDefined shouldBe true
      }
  }
}
