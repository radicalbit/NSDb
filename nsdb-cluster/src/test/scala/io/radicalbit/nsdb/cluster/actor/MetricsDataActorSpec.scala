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

package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.MetricsDataActor.DeleteRecordFromLocation
import io.radicalbit.nsdb.cluster.coordinator.mockedActors.{LocalMetadataCache, LocalMetadataCoordinator}
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbNode}
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.test.NSDbFlatSpecLike
import org.scalatest.BeforeAndAfter

import scala.concurrent.Await
import scala.concurrent.duration._

class MetricsDataActorSpec()
    extends TestKit(ActorSystem("metricsDataActorSpec"))
    with ImplicitSender
    with NSDbFlatSpecLike
    with BeforeAndAfter {

  val probe      = TestProbe()
  val probeActor = probe.ref
  val basePath   = "target/test_index/metricsDataActorSpec"
  val db         = "db"
  val namespace  = "namespace"
  val namespace1 = "namespace1"

  val fakeMetadataCoordinator =
    system.actorOf(LocalMetadataCoordinator.props(system.actorOf(Props[LocalMetadataCache])))

  val metricsDataActor =
    system.actorOf(MetricsDataActor.props(basePath, NSDbNode.empty, ActorRef.noSender))

  private val metric = "metricsDataActorMetric"

  val location = Location(_: String, NSDbNode.empty, 0, 0)

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS) + (1 second)

  before {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = 10 second

    Await.result(metricsDataActor ? DeleteNamespace(db, namespace), 10 seconds)
    Await.result(metricsDataActor ? DeleteNamespace(db, namespace1), 10 seconds)
  }

  "metricsDataActor" should "write and delete properly" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 0.5, Map("dimension" -> s"dimension"), Map("tag" -> s"tag"))

    probe.send(metricsDataActor, AddRecordToShard(db, namespace, location(metric), record))

    val expectedAdd = awaitAssert {
      probe.expectMsgType[RecordAccumulated]
    }
    expectedAdd.metric shouldBe metric
    expectedAdd.record shouldBe record

    expectNoMessage(interval)

    probe.send(metricsDataActor, GetCountWithLocations(db, namespace, metric, Seq(location(metric))))

    val expectedCount = awaitAssert {
      probe.expectMsgType[CountGot]
    }
    expectedCount.metric shouldBe metric
    expectedCount.count shouldBe 1

    probe.send(metricsDataActor, DeleteRecordFromLocation(db, namespace, record, location(metric)))

    val expectedDelete = awaitAssert { probe.expectMsgType[RecordDeleted] }
    expectedDelete.metric shouldBe metric
    expectedDelete.record shouldBe record

    expectNoMessage(interval)

    probe.send(metricsDataActor, GetCountWithLocations(db, namespace, metric, Seq(location(metric))))

    val expectedCountDeleted = awaitAssert { probe.expectMsgType[CountGot] }
    expectedCountDeleted.metric shouldBe metric
    expectedCountDeleted.count shouldBe 0
  }

  "metricsDataActor" should "write and delete properly in multiple namespaces" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 24, Map("dimension" -> s"dimension"), Map("tag" -> s"tag"))

    probe.send(metricsDataActor, AddRecordToShard(db, namespace1, location(metric + "2"), record))

    awaitAssert {
      probe.expectMsgType[RecordAccumulated]
    }

    expectNoMessage(interval)

    probe.send(metricsDataActor, GetCountWithLocations(db, namespace, metric, Seq(location(metric + "2"))))

    val expectedCount = awaitAssert {
      probe.expectMsgType[CountGot]
    }
    expectedCount.metric shouldBe metric
    expectedCount.count shouldBe 0

    probe.send(metricsDataActor, GetCountWithLocations(db, namespace1, metric + "2", Seq(location(metric + "2"))))

    val expectedCount2 = awaitAssert {
      probe.expectMsgType[CountGot]
    }
    expectedCount2.metric shouldBe metric + "2"
    expectedCount2.count shouldBe 1

  }

  "metricsDataActor" should "delete a namespace" in within(5.seconds) {

    val record = Bit(System.currentTimeMillis, 23, Map("dimension" -> s"dimension"), Map("tag" -> s"tag"))

    probe.send(metricsDataActor, AddRecordToShard(db, namespace1, location(metric + "2"), record))

    awaitAssert {
      probe.expectMsgType[RecordAccumulated]
    }

    expectNoMessage(interval)

    probe.send(metricsDataActor, GetCountWithLocations(db, namespace1, metric + "2", Seq(location(metric + "2"))))

    val expectedCount2 = awaitAssert {
      probe.expectMsgType[CountGot]
    }
    expectedCount2.metric shouldBe metric + "2"
    expectedCount2.count shouldBe 1

    probe.send(metricsDataActor, DeleteNamespace(db, namespace1))
    awaitAssert {
      probe.expectMsgType[NamespaceDeleted]
    }

    expectNoMessage(interval)

  }

}
