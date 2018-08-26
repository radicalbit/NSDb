package io.radicalbit.nsdb.cluster

/*
 * Copyright 2018 Radicalbit S.r.l.
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

import java.util.concurrent.TimeUnit

import akka.cluster.{Cluster, MemberStatus}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.PublisherActor.Events.{RecordsPublished, SubscribedByQueryString}
import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.minicluster.MiniClusterSpec
import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter
import io.radicalbit.nsdb.minicluster.ws.WebSocketClient
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class PublishSubscribeClusterSpec extends MiniClusterSpec {

  override val nodesNumber: Int = 2

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  lazy val bit = Bit(timestamp = System.currentTimeMillis(),
                     dimensions = Map("city" -> "Mouseton", "gender" -> "F"),
                     value = 2,
                     tags = Map.empty)

  test("join cluster") {
    assert(
      Cluster(minicluster.nodes.head.system).state.members
        .count(_.status == MemberStatus.Up) == minicluster.nodes.size)
  }

  test("subscribe to a query and receive real time updates") {
    val firstNode  = minicluster.nodes.head
    val secondNode = minicluster.nodes.last

    val nsdbFirstNode =
      Await.result(NSDB.connect(host = "127.0.0.1", port = firstNode.grpcPort)(ExecutionContext.global), 10.seconds)
    val nsdbSecondNode =
      Await.result(NSDB.connect(host = "127.0.0.1", port = secondNode.grpcPort)(ExecutionContext.global), 10.seconds)

    val firstNodeWsClient  = new WebSocketClient("localhost", firstNode.httpPort)
    val secondNodeWsClient = new WebSocketClient("localhost", firstNode.httpPort)

    Await.result(nsdbFirstNode.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
    Await.result(nsdbSecondNode.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)

    waitIndexing()

    //subscription phase
    firstNodeWsClient.subscribe("db", "namespace", "metric1")

    val firstSubscriptionBuffer = firstNodeWsClient.receivedBuffer()

    assert(firstSubscriptionBuffer.length == 1)

    val y = parse(firstSubscriptionBuffer.head.asTextMessage.getStrictText)
    val x = (parse(firstSubscriptionBuffer.head.asTextMessage.getStrictText) \ "records")

    val z = y.extractOpt[Seq[SubscribedByQueryString]]

    val firstSubscribedResponse =  (parse(firstSubscriptionBuffer.head.asTextMessage.getStrictText) \ "records").extractOpt[Seq[Bit]]
    assert(firstSubscribedResponse.isDefined)
    assert(firstSubscribedResponse.get.size == 1)
    assert(firstSubscribedResponse.get.head.timelessEquals(bit))

    secondNodeWsClient.subscribe("db", "namespace", "metric2")

    val secondSubscriptionBuffer = secondNodeWsClient.receivedBuffer()

    assert(secondSubscriptionBuffer.length == 1)

    val secondSubscribedResponse =  parse(secondSubscriptionBuffer.head.asTextMessage.getStrictText).extractOpt[SubscribedByQueryString]
    assert(secondSubscribedResponse.isDefined)
    assert(secondSubscribedResponse.get.records.size == 1)
    assert(secondSubscribedResponse.get.records.head.timelessEquals(bit))

    //streaming phase
    Await.result(nsdbFirstNode.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
    Await.result(nsdbSecondNode.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)


    val firstStreamingBuffer = firstNodeWsClient.receivedBuffer()
    assert(firstStreamingBuffer.length == 1)

    val firstStreamingResponse =  parse(firstStreamingBuffer.head.asTextMessage.getStrictText).extractOpt[RecordsPublished]

    assert(firstStreamingResponse.isDefined)
    assert(firstStreamingResponse.get.records.size == 1)
    assert(firstStreamingResponse.get.records.head.timelessEquals(bit))

    val secondStreamingBuffer = secondNodeWsClient.receivedBuffer()

    assert(firstSubscriptionBuffer.length == 1)

    val secondStreamingResponse =  parse(secondStreamingBuffer.head.asTextMessage.getStrictText).extractOpt[RecordsPublished]

    assert(secondStreamingResponse.isDefined)
    assert(secondStreamingResponse.get.records.size == 1)
    assert(secondStreamingResponse.get.records.head.timelessEquals(bit))

  }

  test("support cross-node subscription and publishing") {
    val firstNode  = minicluster.nodes.head
    val secondNode = minicluster.nodes.last

    val nsdbFirstNode =
      Await.result(NSDB.connect(host = "127.0.0.1", port = firstNode.grpcPort)(ExecutionContext.global), 10.seconds)
    val nsdbSecondNode =
      Await.result(NSDB.connect(host = "127.0.0.1", port = secondNode.grpcPort)(ExecutionContext.global), 10.seconds)

    val firstNodeWsClient  = new WebSocketClient("localhost", firstNode.httpPort)
    val secondNodeWsClient = new WebSocketClient("localhost", firstNode.httpPort)

    //subscription phase
    firstNodeWsClient.subscribe("db", "namespace", "metric1")

    assert(firstNodeWsClient.receivedBuffer().length == 1)

    secondNodeWsClient.subscribe("db", "namespace", "metric2")

    assert(secondNodeWsClient.receivedBuffer().length == 1)

    //streaming phase
    Await.result(nsdbSecondNode.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
    Await.result(nsdbFirstNode.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)

    assert(firstNodeWsClient.receivedBuffer().length == 1)

    assert(secondNodeWsClient.receivedBuffer().length == 1)
  }
}
