/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import akka.actor.{Actor, Props}
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.PublisherActor.Events.SubscribedByQueryString
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteStatement, PublishRecord}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementExecuted
import io.radicalbit.nsdb.security.http.EmptyAuthorization
import io.radicalbit.nsdb.web.actor.StreamActor.QuerystringRegistrationFailed
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class FakeReadCoordinatorActor extends Actor {
  def receive: Receive = {
    case ExecuteStatement(statement) =>
      sender() ! SelectStatementExecuted(statement, values = Seq.empty)
  }
}

class WebSocketSpec() extends FlatSpec with ScalatestRouteTest with Matchers with WsResources {

  override def logger: LoggingAdapter = system.log

  implicit val formats = DefaultFormats

  val basePath = "target/test_index/WebSocketTest"

  val publisherActor = system.actorOf(PublisherActor.props(system.actorOf(Props[FakeReadCoordinatorActor])))

  val wsStandardResources = wsResources(publisherActor, new EmptyAuthorization)

  val wsSecureResources = wsResources(publisherActor, new TestAuthProvider)

  "WebSocketStream" should "fail to register if invalid registration message is provided" in {
    val wsClient = WSProbe()

    WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
      check {

        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage("""{"db":"db","namespace":"a","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        wsClient.expectMessage().asTextMessage.getStrictText shouldBe "\"invalid message sent\""

        wsClient.sendMessage(
          """{"db":"db","namespace":"a","metric":"people","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

        val text = wsClient.expectMessage().asTextMessage.getStrictText

        val obj: Option[QuerystringRegistrationFailed] = parse(text).extractOpt[QuerystringRegistrationFailed]

        obj.isDefined shouldBe true
        obj.get.reason shouldEqual "not a select query"
      }
  }

  "WebSocketStream" should "register to a query and receive events" in {

    val wsClient = WSProbe()

    WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
      check {

        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage(
          """{"db":"db","namespace":"registry","metric":"people","queryString":"select * from people limit 1"}""")

        val firstSubscribed = wsClient.expectMessage().asTextMessage.getStrictText
        parse(firstSubscribed).extractOpt[SubscribedByQueryString].isDefined shouldBe true

        val bit = Bit(System.currentTimeMillis(), 1, Map.empty, Map.empty)
        publisherActor ! PublishRecord("db", "registry", "people", bit, Schema("people", bit))
        publisherActor ! PublishRecord("db", "registry", "animals", bit, Schema("people", bit))

        val firstRecordsPublished = wsClient.expectMessage().asTextMessage.getStrictText

        (parse(firstRecordsPublished) \ "records").extract[JArray].arr.size shouldBe 1
        wsClient.expectNoMessage(5 seconds)
      }
  }

  "Secured WebSocketStream" should "refuse query subscription without a header provided" in {

    val wsClient = WSProbe()

    WS("/ws-stream", wsClient.flow) ~> wsSecureResources ~>
      check {
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage("""{"db":"db","namespace":"a","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

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

    WS("/ws-stream", wsClient.flow, Seq("testHeader")) ~> wsSecureResources ~>
      check {
        isWebSocketUpgrade shouldEqual true

        wsClient.sendMessage("""{"db":"db","namespace":"a","queryString":"INSERT INTO people DIM(name=john) val=23"}""")

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

  "WebSocketStream with publish-period" should "open using default parameter" in {
    val wsClient = WSProbe()
    WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
      check {
        isWebSocketUpgrade shouldEqual true
        response.status shouldBe StatusCodes.SwitchingProtocols
      }
  }
  "WebSocketStream with publish-period" should "open using specified parameter" in {
    val wsClient = WSProbe()
    WS("/ws-stream?refresh_period=500", wsClient.flow) ~> wsStandardResources ~>
      check {
        isWebSocketUpgrade shouldEqual true
        response.status shouldBe StatusCodes.SwitchingProtocols
      }
  }
  "WebSocketStream with publish-period" should "fails with wrong parameter" in {
    val wsClient = WSProbe()
    WS("/ws-stream?refresh_period=10", wsClient.flow) ~> wsStandardResources ~>
      check {
        isWebSocketUpgrade shouldEqual false
        response.status shouldBe StatusCodes.BadRequest
      }
  }
}
