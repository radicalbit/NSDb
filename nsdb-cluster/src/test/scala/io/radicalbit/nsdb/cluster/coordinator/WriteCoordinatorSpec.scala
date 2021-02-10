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
    probe.send(writeCoordinatorActor, SubscribeCommitLogCoordinator(commitLogCoordinator, "localhost"))
    awaitAssert {
      probe.expectMsgType[CommitLogCoordinatorSubscribed]
    }

    probe.send(writeCoordinatorActor, SubscribeMetricsDataActor(metricsDataActor, "localhost"))
    awaitAssert {
      probe.expectMsgType[MetricsDataActorSubscribed]
    }

    probe.send(writeCoordinatorActor, SubscribePublisher(publisherActor, "localhost"))
    awaitAssert {
      probe.expectMsgType[PublisherSubscribed]
    }

    probe.send(schemaCoordinator, UpdateSchemaFromRecord(db, namespace, "testMetric", record1))
    awaitAssert {
      probe.expectMsgType[SchemaUpdated]
    }
  }

  "WriteCoordinator" should behave.like(defaultBehaviour)
}
