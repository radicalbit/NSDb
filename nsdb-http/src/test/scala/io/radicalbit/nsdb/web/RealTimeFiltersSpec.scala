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

import akka.actor.Props
import akka.event.LoggingAdapter
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import io.radicalbit.nsdb.actor.FakeReadCoordinator
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.actors.RealTimeProtocol.Events.{
  RecordsPublished,
  SubscribedByQueryString,
  SubscriptionByQueryStringFailed
}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.PublishRecord
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.web.NSDbJson.RealTimeOutGoingMessageWriter._
import io.radicalbit.nsdb.test.NSDbSpec
import spray.json._

import scala.concurrent.duration._

class RealTimeFiltersSpec extends NSDbSpec with ScalatestRouteTest with WsResources {

  override def logger: LoggingAdapter = system.log

  val basePath = "target/test_index/WebSocketTest"

  val publisherActor = system.actorOf(PublisherActor.props(system.actorOf(Props[FakeReadCoordinator])))

  val wsStandardResources = wsResources(publisherActor, NSDbAuthorizationProvider.empty)

  "Real Time Filter" should {

    "register a query and receive events with a single filter over Long" in {

      val wsClient = WSProbe()

      WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
        check {

          isWebSocketUpgrade shouldEqual true

          wsClient.sendMessage(
            """{
              |"db":"db1",
              |"namespace":"namespace1",
              |"metric":"metric1",
              |"queryString":"select * from metric1 limit 1",
              |"filters" : [{
              |"dimension": "value",
              |"value": 1,
              |"operator": "="
              |}]
              |}""".stripMargin
          )

          val firstSubscribed = wsClient.expectMessage().asTextMessage.getStrictText
          noException should be thrownBy firstSubscribed.parseJson.convertTo[SubscribedByQueryString]

          val bit = Bit(System.currentTimeMillis(), 1, Map.empty, Map.empty)
          publisherActor ! PublishRecord("db1", "namespace1", "metric1", bit, Schema("metric1", bit))
          publisherActor ! PublishRecord("db", "registry", "animals", bit, Schema("people", bit))

          val firstRecordsPublished = wsClient.expectMessage().asTextMessage.getStrictText

          firstRecordsPublished.parseJson.convertTo[RecordsPublished].records.size shouldBe 1
          wsClient.expectNoMessage(5 seconds)
        }
    }

    "register a query and receive events with a single filter over Long with a compatible String filter provided" in {

      val wsClient = WSProbe()

      WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
        check {

          isWebSocketUpgrade shouldEqual true

          wsClient.sendMessage(
            """{
                |"db":"db",
                |"namespace":"registry",
                |"metric":"people",
                |"queryString":"select * from people limit 1",
                |"filters" : [{
                |"dimension": "value",
                |"value": "1",
                |"operator": "="
                |}]
                |}""".stripMargin
          )

          val firstSubscribed = wsClient.expectMessage().asTextMessage.getStrictText
          noException should be thrownBy firstSubscribed.parseJson.convertTo[SubscribedByQueryString]

          val bit = Bit(System.currentTimeMillis(), 1, Map.empty, Map.empty)
          publisherActor ! PublishRecord("db", "registry", "people", bit, Schema("people", bit))
          publisherActor ! PublishRecord("db", "registry", "animals", bit, Schema("people", bit))

          val firstRecordsPublished = wsClient.expectMessage().asTextMessage.getStrictText

          firstRecordsPublished.parseJson.convertTo[RecordsPublished].records.size shouldBe 1
          wsClient.expectNoMessage(5 seconds)
        }
    }

    "register a query and receive events with a single filter over Long with a not compatible String filter provided" in {

      val wsClient = WSProbe()

      WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
        check {

          isWebSocketUpgrade shouldEqual true

          wsClient.sendMessage(
            """{
              |"db":"db",
              |"namespace":"registry",
              |"metric":"people",
              |"queryString":"select * from people limit 1",
              |"filters" : [{
              |"dimension": "value",
              |"value": "vf",
              |"operator": "="
              |}]
              |}""".stripMargin
          )

          val firstSubscribed = wsClient.expectMessage().asTextMessage.getStrictText
          noException should be thrownBy firstSubscribed.parseJson.convertTo[SubscriptionByQueryStringFailed]

          wsClient.expectNoMessage(5 seconds)
        }
    }

    "register a query and receive events with a single filter over Long and a time range" in {

      val wsClient = WSProbe()

      WS("/ws-stream", wsClient.flow) ~> wsStandardResources ~>
        check {

          isWebSocketUpgrade shouldEqual true

          wsClient.sendMessage(
            """{
                |"db":"db1",
                |"namespace":"namespace1",
                |"metric":"metric1",
                |"queryString":"select * from metric1 limit 1",
                |"from": 0,
                |"to": 100,
                |"filters" : [{
                |"dimension": "value",
                |"value": 1,
                |"operator": "="
                |}]
                |}""".stripMargin
          )

          val firstSubscribed = wsClient.expectMessage().asTextMessage.getStrictText
          noException should be thrownBy firstSubscribed.parseJson.convertTo[SubscribedByQueryString]

          val currentBit              = Bit(System.currentTimeMillis(), 1, Map.empty, Map.empty)
          val bitWithNotMatchingValue = Bit(0, 3, Map.empty, Map.empty)
          val matchingBit             = Bit(0, 1, Map.empty, Map.empty)
          publisherActor ! PublishRecord("db1", "namespace1", "metric1", currentBit, Schema("metric1", currentBit))
          publisherActor ! PublishRecord("db1",
                                         "namespace1",
                                         "metric1",
                                         bitWithNotMatchingValue,
                                         Schema("metric1", bitWithNotMatchingValue))
          publisherActor ! PublishRecord("db1", "namespace1", "metric1", matchingBit, Schema("metric1", matchingBit))
          publisherActor ! PublishRecord("db", "registry", "animals", matchingBit, Schema("metric1", matchingBit))

          val firstRecordsPublished = wsClient.expectMessage().asTextMessage.getStrictText

          firstRecordsPublished.parseJson.convertTo[RecordsPublished].records.size shouldBe 1
          wsClient.expectNoMessage(5 seconds)
        }
    }
  }

}
