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

package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest._

import java.util.UUID

class WriteCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "WriteCoordinatorSpec",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
      ))
    with ImplicitSender
    with NSDbSpecLike
    with BeforeAndAfter
    with BeforeAndAfterAll
    with WriteCoordinatorBehaviour {

  lazy val basePath = s"target/test_index/WriteCoordinatorSpec/${UUID.randomUUID}"

  val db        = "writeCoordinatorSpecDB"
  val namespace = "namespace"

  override def beforeAll: Unit = {
    probe.send(writeCoordinatorActor, SubscribeCommitLogCoordinator(commitLogCoordinator, "nodeId"))
    awaitAssert {
      probe.expectMsgType[CommitLogCoordinatorSubscribed]
    }

    probe.send(writeCoordinatorActor, SubscribeMetricsDataActor(metricsDataActor, "nodeId"))
    awaitAssert {
      probe.expectMsgType[MetricsDataActorSubscribed]
    }

    probe.send(writeCoordinatorActor, SubscribePublisher(publisherActor, "nodeId"))
    awaitAssert {
      probe.expectMsgType[PublisherSubscribed]
    }

    probe.send(schemaCoordinator, UpdateSchemaFromRecord(db, namespace, "testMetric", record1))
    awaitAssert {
      probe.expectMsgType[SchemaUpdated]
    }
  }

  "WriteCoordinator" should behave.like(defaultBehaviour)

  //  "WriteCoordinator" should {
//    "write records" in {
//      val record1 = Bit(System.currentTimeMillis, 1, Map("content" -> s"content"), Map.empty)
//      val record2 =
//        Bit(System.currentTimeMillis, 2, Map("content" -> s"content", "content2" -> s"content2"), Map.empty)
//      val incompatibleRecord =
//        Bit(System.currentTimeMillis, 3, Map("content" -> 1, "content2" -> s"content2"), Map.empty)
//
//      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record1))
//
//      val expectedAdd = awaitAssert {
//        probe.expectMsgType[InputMapped]
//      }
//      expectedAdd.metric shouldBe "testMetric"
//      expectedAdd.record shouldBe record1
//
//      probe.send(writeCoordinatorActor, MapInput(System.currentTimeMillis, db, namespace, "testMetric", record2))
//
//      val expectedAdd2 = awaitAssert {
//        probe.expectMsgType[InputMapped]
//      }
//      expectedAdd2.metric shouldBe "testMetric"
//      expectedAdd2.record shouldBe record2
//
//      probe.send(writeCoordinatorActor,
//        MapInput(System.currentTimeMillis, db, namespace, "testMetric", incompatibleRecord))
//
//      awaitAssert {
//        probe.expectMsgType[RecordRejected]
//      }
//
//    }
//  }
}
