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

package io.radicalbit.nsdb.actors

import java.nio.file.{Files, Paths}
import java.util.UUID

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.MetricPerformerActor.PerformShardWrites
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Location
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class MetricPerformerActorSpec
    extends TestKit(ActorSystem("IndexerActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val basePath                   = "target/test_index"
  val db                         = "db"
  val namespace                  = "namespace"
  val localWriteCoordinator      = TestProbe()
  val localWriteCoordinatorActor = localWriteCoordinator.ref
  val indexerPerformerActor =
    TestActorRef[MetricPerformerActor](MetricPerformerActor.props(basePath, db, namespace, localWriteCoordinatorActor),
                                       probeActor)

  before {
    import scala.collection.JavaConverters._
    if (Paths.get(basePath, db).toFile.exists())
      Files.walk(Paths.get(basePath, db)).iterator().asScala.map(_.toFile).toSeq.reverse.foreach(_.delete)
  }

  "ShardPerformerActor" should "write and delete properly" in within(5.seconds) {

    val key = Location("IndexerPerformerActorMetric", "node1", 0, 0)

    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"), Map.empty)

    val operations =
      Map(UUID.randomUUID().toString -> WriteShardOperation(namespace, key, bit))

    probe.send(indexerPerformerActor, PerformShardWrites(operations))
    awaitAssert {
      probe.expectMsgType[Refresh]
    }

    awaitAssert {
      val msg = localWriteCoordinator.expectMsgType[MetricPerformerActor.PersistedBits]
      msg.persistedBits.size shouldBe 1
    }

  }
}
