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

package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.{AddRecordToLocation, DeleteRecordFromLocation}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class MetricsDataActorSpec()
    extends TestKit(ActorSystem("namespaceActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe            = TestProbe()
  val probeActor       = probe.ref
  val basePath         = "target/test_index/NamespaceActorSpec"
  val db               = "db"
  val namespace        = "namespace"
  val namespace1       = "namespace1"
  val metricsDataActor = system.actorOf(MetricsDataActor.props(basePath))

  private val metric = "namespaceActorMetric"

  val location = Location(_: String, "testNode", 0, 0)

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + (1 second)

  before {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = 10 second

    Await.result(metricsDataActor ? DeleteNamespace(db, namespace), 3 seconds)
    Await.result(metricsDataActor ? DeleteNamespace(db, namespace1), 3 seconds)
  }

  "namespaceActor" should "write and delete properly" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 0.5, Map("content" -> s"content"))

    probe.send(metricsDataActor, AddRecordToLocation(db, namespace, record, location(metric)))

    awaitAssert {
      val expectedAdd = probe.expectMsgType[RecordAdded]
      expectedAdd.metric shouldBe metric
      expectedAdd.record shouldBe record
    }

    expectNoMessage(interval)

    probe.send(metricsDataActor, GetCount(db, namespace, metric))

    awaitAssert {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe metric
      expectedCount.count shouldBe 1
    }

    probe.send(metricsDataActor, DeleteRecordFromLocation(db, namespace, record, location(metric)))

    awaitAssert {
      val expectedDelete = probe.expectMsgType[RecordDeleted]
      expectedDelete.metric shouldBe metric
      expectedDelete.record shouldBe record
    }

    expectNoMessage(interval)

    probe.send(metricsDataActor, GetCount(db, namespace, metric))

    awaitAssert {
      val expectedCountDeleted = probe.expectMsgType[CountGot]
      expectedCountDeleted.metric shouldBe metric
      expectedCountDeleted.count shouldBe 0
    }

  }

  "namespaceActor" should "write and delete properly in multiple namespaces" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 24, Map("content" -> s"content"))

    probe.send(metricsDataActor, AddRecordToLocation(db, namespace1, record, location(metric + "2")))
    probe.expectMsgType[RecordAdded]

    expectNoMessage(interval)

    probe.send(metricsDataActor, GetCount(db, namespace, metric))

    awaitAssert {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe metric
      expectedCount.count shouldBe 0
    }

    probe.send(metricsDataActor, GetCount(db, namespace1, metric + "2"))

    awaitAssert {
      val expectedCount2 = probe.expectMsgType[CountGot]
      expectedCount2.metric shouldBe metric + "2"
      expectedCount2.count shouldBe 1
    }

  }

  "namespaceActor" should "delete a namespace" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 23, Map("content" -> s"content"))

    probe.send(metricsDataActor, AddRecordToLocation(db, namespace1, record, location(metric + "2")))
    probe.expectMsgType[RecordAdded]

    expectNoMessage(interval)

    probe.send(metricsDataActor, GetCount(db, namespace1, metric + "2"))
    awaitAssert {
      val expectedCount2 = probe.expectMsgType[CountGot]
      expectedCount2.metric shouldBe metric + "2"
      expectedCount2.count shouldBe 1
    }

    probe.send(metricsDataActor, DeleteNamespace(db, namespace1))
    awaitAssert {
      probe.expectMsgType[NamespaceDeleted]
    }

    expectNoMessage(interval)

  }

}
