package io.radicalbit.nsdb.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.PublisherActor.Events.{SubscribedByQueryString, SubscribedByQuid}
import io.radicalbit.nsdb.index.{Schema, VARCHAR}
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteStatement, GetSchema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{SchemaGot, SelectStatementExecuted}
import io.radicalbit.nsdb.security.http.EmptyAuthorization
import io.radicalbit.nsdb.web.actor.StreamActor.QuerystringRegistrationFailed
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

class FakeReadCoordinatorActor extends Actor {
  def receive: Receive = {
    case ExecuteStatement(_) =>
      sender() ! SelectStatementExecuted(db = "db", namespace = "registry", metric = "people", values = Seq.empty)
  }
}

class FakeNamespaceSchemaActor extends Actor {
  def receive: Receive = {
    case GetSchema(_, _, _) =>
      sender() ! SchemaGot(db = "db",
                           namespace = "registry",
                           metric = "people",
                           schema = Some(Schema("people", Seq(SchemaField("surname", VARCHAR())))))
  }
}

class WebSocketTest() extends FlatSpec with ScalatestRouteTest with Matchers with WsResources {

  implicit val formats = DefaultFormats

  val basePath = "target/test_index/WebSocketTest"

  val publisherActor = system.actorOf(
    PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor]),
                         system.actorOf(Props[FakeNamespaceSchemaActor])))
  val wsClient = WSProbe()

  "WebSocketStream" should "register to a query" in {
    WS("/ws-stream", wsClient.flow) ~> wsResources(publisherActor, new EmptyAuthorization) ~>
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
}
