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
import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter
import io.radicalbit.nsdb.minicluster.ws.WebSocketClient
import io.radicalbit.nsdb.test.MiniClusterSpec
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class PublishSubscribeClusterSpec extends MiniClusterSpec {

  override val nodesNumber: Int = 3

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  lazy val bit = Bit(timestamp = System.currentTimeMillis(),
                     dimensions = Map("city" -> "Mouseton", "gender" -> "F"),
                     value = 2,
                     tags = Map.empty)

  test("join cluster") {
    eventually {
      assert(
        Cluster(minicluster.nodes.head.system).state.members
          .count(_.status == MemberStatus.Up) == minicluster.nodes.size)
    }
  }

  test("subscribe to a query and receive real time updates") {
    val firstNode  = minicluster.nodes.head
    val secondNode = minicluster.nodes.last

    val nsdbFirstNode =
      eventually {
        Await.result(NSDB.connect(host = "127.0.0.1", port = firstNode.grpcPort)(ExecutionContext.global), 10.seconds)
      }
    val nsdbSecondNode =
      eventually {
        Await.result(NSDB.connect(host = "127.0.0.1", port = secondNode.grpcPort)(ExecutionContext.global), 10.seconds)
      }

    val firstNodeWsClient  = new WebSocketClient("localhost", firstNode.httpPort)
    val secondNodeWsClient = new WebSocketClient("localhost", firstNode.httpPort)

    eventually {
      assert(
        Await.result(nsdbFirstNode.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds).completedSuccessfully)
      assert(
        Await
          .result(nsdbSecondNode.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)
          .completedSuccessfully)
    }
    waitIndexing()

    //subscription phase
    eventually {
      firstNodeWsClient.subscribe("db", "namespace", "metric1")

      val firstSubscriptionBuffer = firstNodeWsClient.receivedBuffer()

      assert(firstSubscriptionBuffer.length == 1)

      val firstSubscriptionResponse =
        parse(firstSubscriptionBuffer.head.asTextMessage.getStrictText)

      assert((firstSubscriptionResponse \ "records").extract[JArray].arr.size == 1)

      secondNodeWsClient.subscribe("db", "namespace", "metric2")

      val secondSubscriptionBuffer = secondNodeWsClient.receivedBuffer()

      assert(secondSubscriptionBuffer.length == 1)

      val secondSubscriptionResponse =
        parse(secondSubscriptionBuffer.head.asTextMessage.getStrictText)

      assert((secondSubscriptionResponse \ "records").extract[JArray].arr.size == 1)

      //streaming phase
      Await.result(nsdbFirstNode.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
      Await.result(nsdbSecondNode.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)

      val firstStreamingBuffer = firstNodeWsClient.receivedBuffer()
      assert(firstStreamingBuffer.length == 1)

      val firstStreamingResponse =
        parse(firstStreamingBuffer.head.asTextMessage.getStrictText)

      assert((firstStreamingResponse \ "metric").extract[String] == "metric1")
      assert((firstStreamingResponse \ "records").extract[JArray].arr.size == 1)

      val secondStreamingBuffer = secondNodeWsClient.receivedBuffer()

      assert(firstSubscriptionBuffer.length == 1)

      val secondStreamingResponse =
        parse(secondStreamingBuffer.head.asTextMessage.getStrictText)

      assert((secondStreamingResponse \ "metric").extract[String] == "metric2")
      assert((secondStreamingResponse \ "records").extract[JArray].arr.size == 1)
    }
  }

  test("support cross-node subscription and publishing") {
    val firstNode  = minicluster.nodes.head
    val secondNode = minicluster.nodes.last

    val nsdbFirstNode = eventually {
      Await.result(NSDB.connect(host = "127.0.0.1", port = firstNode.grpcPort)(ExecutionContext.global), 10.seconds)
    }
    val nsdbSecondNode = eventually {
      Await.result(NSDB.connect(host = "127.0.0.1", port = secondNode.grpcPort)(ExecutionContext.global), 10.seconds)
    }

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
