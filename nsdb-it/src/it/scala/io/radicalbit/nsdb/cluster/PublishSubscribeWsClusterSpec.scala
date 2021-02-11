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

package io.radicalbit.nsdb.cluster

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

import java.util.concurrent.TimeUnit

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

class PublishSubscribeWsClusterSpec extends MiniClusterSpec {

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  lazy val bit: Bit = Bit(timestamp = System.currentTimeMillis(),
                          dimensions = Map("city" -> "Mouseton", "gender" -> "F"),
                          value = 2,
                          tags = Map.empty)

  test("subscribe to a query and receive real time updates") {
    val firstNode = nodes.head
    val lastNode  = nodes.last

    val nsdbFirstNode =
      eventually {
        Await.result(NSDB.connect(host = firstNode.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      }
    val nsdbLastNode =
      eventually {
        Await.result(NSDB.connect(host = lastNode.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
      }

    val firstNodeWsClient = new WebSocketClient("127.0.0.1", 9000)
    val lastNodeWsClient  = new WebSocketClient("127.0.0.3", 9000)

    eventually {
      assert(
        Await
          .result(nsdbFirstNode.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
          .completedSuccessfully)
    }
    eventually {
      assert(
        Await
          .result(nsdbLastNode.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)
          .completedSuccessfully)
    }

    waitIndexing()

    firstNodeWsClient.subscribe("db", "namespace", "metric1")
    //subscription phase
    val firstSubscriptionBuffer = eventually {
      val firstSubscriptionBuffer = firstNodeWsClient.receivedBuffer()
      assert(firstSubscriptionBuffer.length == 1)
      firstSubscriptionBuffer
    }

    val firstSubscriptionResponse =
      parse(firstSubscriptionBuffer.head.asTextMessage.getStrictText)

    assert((firstSubscriptionResponse \ "records").extract[JArray].arr.size == 1)

    lastNodeWsClient.subscribe("db", "namespace", "metric2")
    val lastSubscriptionBuffer = eventually {

      val lastSubscriptionBuffer = lastNodeWsClient.receivedBuffer()

      assert(lastSubscriptionBuffer.length == 1)

      lastSubscriptionBuffer
    }

    val lastSubscriptionResponse =
      parse(lastSubscriptionBuffer.head.asTextMessage.getStrictText)

    assert((lastSubscriptionResponse \ "records").extract[JArray].arr.size == 1)

    //streaming phase
    firstNodeWsClient.clearBuffer()
    lastNodeWsClient.clearBuffer()
    eventually {
      assert(
        Await
          .result(nsdbFirstNode.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
          .completedSuccessfully)
    }

    eventually {
      assert(
        Await
          .result(nsdbLastNode.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)
          .completedSuccessfully)
    }

    val firstStreamingBuffer = eventually {
      val firstStreamingBuffer = firstNodeWsClient.receivedBuffer()
      assert(firstStreamingBuffer.length == 1)
      firstStreamingBuffer
    }

    val firstStreamingResponse =
      parse(firstStreamingBuffer.head.asTextMessage.getStrictText)

    assert((firstStreamingResponse \ "metric").extract[String] == "metric1")
    assert((firstStreamingResponse \ "records").extract[JArray].arr.size == 1)

    val lastStreamingBuffer = eventually {
      val lastStreamingBuffer = lastNodeWsClient.receivedBuffer()
      assert(lastStreamingBuffer.length == 1)
      lastStreamingBuffer
    }

    val lastStreamingResponse = parse(lastStreamingBuffer.head.asTextMessage.getStrictText)

    assert((lastStreamingResponse \ "metric").extract[String] == "metric2")
    assert((lastStreamingResponse \ "records").extract[JArray].arr.size == 1)

  }

  test("support cross-node subscription and publishing") {
    val firstNode = nodes.head
    val lastNode  = nodes.last

    val nsdbFirstNode = eventually {
      Await.result(NSDB.connect(host = firstNode.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
    }
    val nsdbLastNode = eventually {
      Await.result(NSDB.connect(host = lastNode.hostname, port = 7817)(ExecutionContext.global), 10.seconds)
    }

    val firstNodeWsClient = new WebSocketClient("127.0.0.1", 9000)
    val lastNodeWsClient  = new WebSocketClient("127.0.0.3", 9000)

    //subscription phase
    firstNodeWsClient.subscribe("db", "namespace", "metric1")
    lastNodeWsClient.subscribe("db", "namespace", "metric2")

    eventually {
      assert(firstNodeWsClient.receivedBuffer().length == 1)
    }

    eventually {
      assert(lastNodeWsClient.receivedBuffer().length == 1)
    }

    //streaming phase
    eventually {
      assert(
        Await
          .result(nsdbLastNode.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
          .completedSuccessfully)
    }
    eventually {
      assert(
        Await
          .result(nsdbFirstNode.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)
          .completedSuccessfully)
    }

    eventually {
      assert(firstNodeWsClient.receivedBuffer().length == 1)
    }
    eventually {
      assert(lastNodeWsClient.receivedBuffer().length == 1)
    }
  }
}
