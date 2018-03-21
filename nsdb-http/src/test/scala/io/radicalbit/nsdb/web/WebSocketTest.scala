package io.radicalbit.nsdb.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.PublisherActor.Events.{SubscribedByQueryString, SubscribedByQuid}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementExecuted
import io.radicalbit.nsdb.security.http.EmptyAuthorization
import io.radicalbit.nsdb.web.actor.StreamActor.QuerystringRegistrationFailed
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

class FakeReadCoordinatorActor extends Actor {
  def receive: Receive = {
    case ExecuteStatement(_) =>
      sender() ! SelectStatementExecuted(db = "db", namespace = "registry", metric = "people", values = Seq.empty)
  }
}

class WebSocketTest() extends FlatSpec with ScalatestRouteTest with Matchers with WsResources {

  implicit val formats = DefaultFormats

  val basePath = "target/test_index/WebSocketTest"

  val publisherActor = system.actorOf(PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor])))

  val wsStandardResources = wsResources(publisherActor, new EmptyAuthorization)

  val wsSecureResources = wsResources(publisherActor, new TestAuthProvider)

  "WebSocketStream" should "register to a query" in {

    val wsClient = WSProbe()

    WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
      check {

        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        wsClient.expectMessage().asTextMessage.getStrictText shouldBe "\"invalid message sent\""

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","metric":"people","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        val text = wsClient.expectMessage().asTextMessage.getStrictText

        val obj: Option[QuerystringRegistrationFailed] = parse(text).extractOpt[QuerystringRegistrationFailed]

        obj.isDefined shouldBe true
        obj.get.reason shouldEqual "not a select query"

        wsClient.sendMessage(
          """{"db":"db","namespace":"registry","metric":"people","queryString":"select * from people limit 1"}""")

        val subscribed = wsClient.expectMessage().asTextMessage.getStrictText
        parse(subscribed).extractOpt[SubscribedByQueryString].isDefined shouldBe true

        //TODO find out how to test combining somehow the actorsystem coming from ScalatestRouteTest and from Testkit
      }
  }

  "WebSocketStream" should "register to a queryID" in {

    val wsClient = WSProbe()

    WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
      check {

        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage(
          """{"db":"db","namespace":"registry","metric":"people","queryString":"select * from people limit 1"}""")

        val subscribed = wsClient.expectMessage().asTextMessage.getStrictText
        parse(subscribed).extractOpt[SubscribedByQueryString].isDefined shouldBe true
        val response = parse(subscribed).extractOpt[SubscribedByQueryString].get

        wsClient.sendMessage(
          s"""{"db":"db","namespace":"registry","metric":"people","quid":${response.quid}} """
        )
        val subscribedQId = wsClient.expectMessage().asTextMessage.getStrictText
        parse(subscribed).extractOpt[SubscribedByQuid].isDefined shouldBe true

        //TODO find out how to test combining somehow the actorsystem coming from ScalatestRouteTest and from Testkit
      }
  }

  "Secured WebSocketStream" should "refuse query subscription without a header provided" in {

    val wsClient = WSProbe()

    WS("/ws-stream", wsClient.flow) ~> wsSecureResources ~>
      check {
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        wsClient.expectMessage().asTextMessage.getStrictText shouldBe "\"invalid message sent\""

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","metric":"people","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        val text = wsClient.expectMessage().asTextMessage.getStrictText

        val obj: Option[QuerystringRegistrationFailed] = parse(text).extractOpt[QuerystringRegistrationFailed]

        obj.isDefined shouldBe true
        obj.get.reason shouldEqual "unauthorized header not provided"

      }
  }

  "Secured WebSocketStream" should "refuse query subscription with a wrong header provided" in {

    val wsClient = WSProbe()

    val wsHeader = WS("/ws-stream", wsClient.flow).headers.head

    WS("/ws-stream", wsClient.flow).withHeaders(wsHeader, RawHeader("wrong", "wrong")) ~> wsSecureResources ~>
      check {
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","metric":"people","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        val text = wsClient.expectMessage().asTextMessage.getStrictText

        val obj: Option[QuerystringRegistrationFailed] = parse(text).extractOpt[QuerystringRegistrationFailed]

        obj.isDefined shouldBe true
        obj.get.reason shouldEqual "unauthorized header not provided"

      }
  }

  "Secured WebSocketStream" should "refuse query for an unauthorized metric" in {

    val wsClient = WSProbe()

    val wsHeader = WS("/ws-stream", wsClient.flow).headers.head

    WS("/ws-stream", wsClient.flow)
      .withHeaders(wsHeader, RawHeader("testHeader", "testHeader")) ~> wsSecureResources ~>
      check {
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        wsClient.expectMessage().asTextMessage.getStrictText shouldBe "\"invalid message sent\""

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","metric":"people","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        val notSelect: Option[QuerystringRegistrationFailed] =
          parse(wsClient.expectMessage().asTextMessage.getStrictText).extractOpt[QuerystringRegistrationFailed]

        notSelect.isDefined shouldBe true
        notSelect.get.reason shouldEqual "not a select query"

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","metric":"notAuthorizedMetric","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        val text = wsClient.expectMessage().asTextMessage.getStrictText

        val obj: Option[QuerystringRegistrationFailed] = parse(text).extractOpt[QuerystringRegistrationFailed]

        obj.isDefined shouldBe true
        obj.get.reason shouldEqual "unauthorized forbidden access to metric notAuthorizedMetric"

        wsClient.sendMessage(
          """{"db":"db","namespace":"registry","metric":"people","queryString":"select * from people limit 1"}""")

        val subscribed = wsClient.expectMessage().asTextMessage.getStrictText
        parse(subscribed).extractOpt[SubscribedByQueryString].isDefined shouldBe true
      }
  }
}
