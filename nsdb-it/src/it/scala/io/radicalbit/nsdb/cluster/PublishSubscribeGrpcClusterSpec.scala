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

import akka.util.Timeout
import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.api.scala.streaming.NSDbStreaming._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.minicluster.converters.BitConverters.BitConverter
import io.radicalbit.nsdb.rpc.streaming.SQLStreamingResponse
import io.radicalbit.nsdb.test.MiniClusterSpec

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class PublishSubscribeGrpcClusterSpec extends MiniClusterSpec {

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  lazy val bit: Bit = Bit(timestamp = System.currentTimeMillis(),
                          dimensions = Map("city" -> "Mouseton", "gender" -> "F"),
                          value = 2,
                          tags = Map.empty)

  test("subscribe to a query and receive real time updates") {
    val nsdbFirstNodeConnection =
      eventually {
        Await.result(NSDB.connect(host = firstNode.hostname, port = 7817), 10.seconds)
      }
    val nsdbLastNodeConnection =
      eventually {
        Await.result(NSDB.connect(host = lastNode.hostname, port = 7817), 10.seconds)
      }

    eventually {
      assert(
        Await
          .result(nsdbFirstNodeConnection.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
          .completedSuccessfully)
    }
    eventually {
      assert(
        Await
          .result(nsdbFirstNodeConnection.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)
          .completedSuccessfully)
    }

    waitIndexing()

    val firstNodeSubscriptionBuffer = ListBuffer.empty[SQLStreamingResponse]
    nsdbFirstNodeConnection.subscribe(
      nsdbFirstNodeConnection.db("db").namespace("namespace").metric("metric1").query("select * from metric1 limit 1")) {
      firstNodeSubscriptionBuffer += _
    }

    val lastNodeSubscriptionBuffer = ListBuffer.empty[SQLStreamingResponse]
    nsdbLastNodeConnection.subscribe(
      nsdbLastNodeConnection.db("db").namespace("namespace").metric("metric2").query("select * from metric2 limit 1")) {
      lastNodeSubscriptionBuffer += _
    }

    //subscription phase
    eventually{
      firstNodeSubscriptionBuffer.size == 1
      firstNodeSubscriptionBuffer.head.db == "db"
      firstNodeSubscriptionBuffer.head.namespace == "namespace"
      firstNodeSubscriptionBuffer.head.metric == "metric1"
      firstNodeSubscriptionBuffer.head.payload.isSubscribedByQueryString
      firstNodeSubscriptionBuffer.head.payload.subscribedByQueryString.get.records.size == 1
      firstNodeSubscriptionBuffer.head.payload.subscribedByQueryString.get.quid
    }

    eventually{
      lastNodeSubscriptionBuffer.size == 1
      lastNodeSubscriptionBuffer.head.db == "db"
      lastNodeSubscriptionBuffer.head.namespace == "namespace"
      lastNodeSubscriptionBuffer.head.metric == "metric2"
      lastNodeSubscriptionBuffer.head.payload.isSubscribedByQueryString
      lastNodeSubscriptionBuffer.head.payload.subscribedByQueryString.get.records.size == 1
      lastNodeSubscriptionBuffer.head.payload.subscribedByQueryString.get.quid
    }


    //streaming phase
    firstNodeSubscriptionBuffer.clear()
    lastNodeSubscriptionBuffer.clear()

    eventually {
      assert(
        Await
          .result(nsdbFirstNodeConnection.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
          .completedSuccessfully)
    }

    eventually {
      assert(
        Await
          .result(nsdbLastNodeConnection.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)
          .completedSuccessfully)
    }

    eventually{
      firstNodeSubscriptionBuffer.size == 1
      firstNodeSubscriptionBuffer.head.db == "db"
      firstNodeSubscriptionBuffer.head.namespace == "namespace"
      firstNodeSubscriptionBuffer.head.metric == "metric1"
      firstNodeSubscriptionBuffer.head.payload.isRecordsPublished
      firstNodeSubscriptionBuffer.head.payload.recordsPublished.get.records.size == 1
    }

    eventually{
      lastNodeSubscriptionBuffer.size == 1
      lastNodeSubscriptionBuffer.head.db == "db"
      lastNodeSubscriptionBuffer.head.namespace == "namespace"
      lastNodeSubscriptionBuffer.head.metric == "metric2"
      lastNodeSubscriptionBuffer.head.payload.isRecordsPublished
      lastNodeSubscriptionBuffer.head.payload.recordsPublished.get.records.size == 1
    }

  }


  test("support cross-node subscription and publishing") {

    val nsdbFirstNodeConnection =
      eventually {
        Await.result(NSDB.connect(host = firstNode.hostname, port = 7817), 10.seconds)
      }
    val nsdbLastNodeConnection =
      eventually {
        Await.result(NSDB.connect(host = lastNode.hostname, port = 7817), 10.seconds)
      }

    val firstNodeSubscriptionBuffer = ListBuffer.empty[SQLStreamingResponse]
    nsdbFirstNodeConnection.subscribe(
      nsdbFirstNodeConnection.db("db").namespace("namespace").metric("metric1").query("select * from metric1 limit 1")) {
      firstNodeSubscriptionBuffer += _
    }

    val lastNodeSubscriptionBuffer = ListBuffer.empty[SQLStreamingResponse]
    nsdbLastNodeConnection.subscribe(
      nsdbLastNodeConnection.db("db").namespace("namespace").metric("metric2").query("select * from metric2 limit 1")) {
      lastNodeSubscriptionBuffer += _
    }

    //subscription phase
    eventually{
      firstNodeSubscriptionBuffer.size == 1
      firstNodeSubscriptionBuffer.head.db == "db"
      firstNodeSubscriptionBuffer.head.namespace == "namespace"
      firstNodeSubscriptionBuffer.head.metric == "metric1"
      firstNodeSubscriptionBuffer.head.payload.isSubscribedByQueryString
      firstNodeSubscriptionBuffer.head.payload.subscribedByQueryString.get.records.size == 1
      firstNodeSubscriptionBuffer.head.payload.subscribedByQueryString.get.quid
    }

    eventually{
      lastNodeSubscriptionBuffer.size == 1
      lastNodeSubscriptionBuffer.head.db == "db"
      lastNodeSubscriptionBuffer.head.namespace == "namespace"
      lastNodeSubscriptionBuffer.head.metric == "metric2"
      lastNodeSubscriptionBuffer.head.payload.isSubscribedByQueryString
      lastNodeSubscriptionBuffer.head.payload.subscribedByQueryString.get.records.size == 1
      lastNodeSubscriptionBuffer.head.payload.subscribedByQueryString.get.quid
    }

    //streaming phase: write request comes from different node than the subscribed ones
    firstNodeSubscriptionBuffer.clear()
    lastNodeSubscriptionBuffer.clear()

    eventually {
      assert(
        Await
          .result(nsdbLastNodeConnection.write(bit.asApiBit("db", "namespace", "metric1")), 10.seconds)
          .completedSuccessfully)
    }
    eventually {
      assert(
        Await
          .result(nsdbFirstNodeConnection.write(bit.asApiBit("db", "namespace", "metric2")), 10.seconds)
          .completedSuccessfully)
    }

    eventually{
      firstNodeSubscriptionBuffer.size == 1
      firstNodeSubscriptionBuffer.head.db == "db"
      firstNodeSubscriptionBuffer.head.namespace == "namespace"
      firstNodeSubscriptionBuffer.head.metric == "metric1"
      firstNodeSubscriptionBuffer.head.payload.isRecordsPublished
      firstNodeSubscriptionBuffer.head.payload.recordsPublished.get.records.size == 1
    }

    eventually{
      lastNodeSubscriptionBuffer.size == 1
      lastNodeSubscriptionBuffer.head.db == "db"
      lastNodeSubscriptionBuffer.head.namespace == "namespace"
      lastNodeSubscriptionBuffer.head.metric == "metric2"
      lastNodeSubscriptionBuffer.head.payload.isRecordsPublished
      lastNodeSubscriptionBuffer.head.payload.recordsPublished.get.records.size == 1
    }
  }

}
