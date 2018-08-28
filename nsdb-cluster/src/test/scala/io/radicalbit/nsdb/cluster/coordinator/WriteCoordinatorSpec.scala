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

package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.commands.WarmUpSchemas
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.WarmUpCompleted
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class WriteCoordinatorSpec
    extends TestKit(
      ActorSystem(
        "nsdb-test",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
      ))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with WriteCoordinatorBehaviour {

  lazy val basePath = "target/test_index/WriteCoordinatorSpec"

  val db        = "writeCoordinatorSpecDB"
  val namespace = "namespace"

  implicit val timeout = Timeout(10 seconds)

  override def beforeAll: Unit = {
    writeCoordinatorActor ! WarmUpCompleted
    schemaCoordinator ! WarmUpSchemas(List.empty)
    Await.result(writeCoordinatorActor ? SubscribeCommitLogCoordinator(commitLogCoordinator, "node1"), 10 seconds)
    Await.result(writeCoordinatorActor ? SubscribeMetricsDataActor(metricsDataActor, "node1"), 10 seconds)
    Await.result(writeCoordinatorActor ? SubscribePublisher(publisherActor, "node1"), 10 seconds)
    Await.result(writeCoordinatorActor ? DeleteNamespace(db, namespace), 10 seconds)
    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, "testMetric", record1), 10 seconds)
  }

  "WriteCoordinator" should behave.like(defaultBehaviour)
}
