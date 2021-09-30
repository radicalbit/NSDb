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

package io.radicalbit.nsdb.actors

import akka.actor.SupervisorStrategy.Resume
import akka.actor._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.MetricPerformerActor.PerformShardWrites
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbNode}
import io.radicalbit.nsdb.exception.InvalidLocationsInNode
import io.radicalbit.nsdb.index.BrokenTimeSeriesIndex
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.nio.file.Paths
import java.util.UUID
import scala.collection.mutable.ListBuffer

class TestSupervisorActor(probe: ActorRef) extends Actor with ActorLogging {

  val exceptionsCaught: ListBuffer[InvalidLocationsInNode] = ListBuffer.empty

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1) {
    case e: InvalidLocationsInNode =>
      log.error(e, s"Invalid locations ${e.locations} found")
      exceptionsCaught += e
      Resume
    case t =>
      log.error(t, "generic exception")
      Resume
  }

  override def receive: Receive = {
    case msg => probe ! msg
  }
}

class MetricPerformerActorSpec
    extends TestKit(ActorSystem("IndexerActorSpec"))
    with ImplicitSender
    with NSDbSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val basePath                  = s"target/MetricPerformerActorSpec/${UUID.randomUUID()}"
  val db                        = "db"
  val namespace                 = "namespace"
  val localCommitLogCoordinator = TestProbe()

  val testSupervisor                 = TestActorRef[TestSupervisorActor](Props(new TestSupervisorActor(probeActor)))
  val localCommitLogCoordinatorActor = localCommitLogCoordinator.ref
  val indexerPerformerActor =
    TestActorRef[MetricPerformerActor](
      MetricPerformerActor.props(basePath, db, namespace, localCommitLogCoordinatorActor),
      testSupervisor,
      "indexerPerformerActor")

  indexerPerformerActor.underlyingActor.supervisorStrategy

  val node = NSDbNode("node1", "node1", "node1")

  val errorLocation               = Location("IndexerPerformerActorMetric", node, 1, 1)
  val unstableRecoverableLocation = Location("IndexerPerformerActorMetric", node, 2, 2)
  val unstableLocation            = Location("IndexerPerformerActorMetric", node, 3, 3)
  val bit                         = Bit(System.currentTimeMillis, 25, Map("content" -> "content"), Map.empty)

  override def beforeAll: Unit = {

    def directoryFromLocation(location: Location) =
      new MMapDirectory(
        Paths
          .get(basePath, db, namespace, "shards", s"${location.shardName}"))

    indexerPerformerActor.underlyingActor.shards += (errorLocation -> new BrokenTimeSeriesIndex(
      directory = directoryFromLocation(errorLocation)))
    indexerPerformerActor.underlyingActor.shards += (unstableRecoverableLocation -> new BrokenTimeSeriesIndex(
      failureBeforeSuccess = 2,
      directory = directoryFromLocation(unstableRecoverableLocation)))
    indexerPerformerActor.underlyingActor.shards += (unstableLocation -> new BrokenTimeSeriesIndex(
      failureBeforeSuccess = 5,
      directory = directoryFromLocation(unstableLocation)))
  }

  override def beforeEach: Unit = {
    testSupervisor.underlyingActor.exceptionsCaught.clear()
    testSupervisor.underlyingActor.exceptionsCaught.size shouldBe 0

    indexerPerformerActor.underlyingActor.toRetryOperations.clear()
    indexerPerformerActor.underlyingActor.toRetryOperations.size shouldBe 0
  }

  "ShardPerformerActor" should {
    "write and delete properly" in {

      val loc = Location("IndexerPerformerActorMetric", node, 0, 0)

      val operations =
        Map(UUID.randomUUID().toString -> WriteShardOperation(namespace, loc, bit))

      probe.send(indexerPerformerActor, PerformShardWrites(operations))
      awaitAssert {
        probe.expectMsgType[Refresh]
      }

      awaitAssert {
        val msg = localCommitLogCoordinator.expectMsgType[MetricPerformerActor.PersistedBits]
        msg.persistedBits.size shouldBe 1
      }

    }

    "retry and fail in case of write error" in {

      val writeOperation =
        Map(UUID.randomUUID().toString -> WriteShardOperation(namespace, errorLocation, bit))

      probe.send(indexerPerformerActor, PerformShardWrites(writeOperation))
      awaitAssert {
        probe.expectMsgType[Refresh]
      }

      awaitAssert {
        val msg = localCommitLogCoordinator.expectMsgType[MetricPerformerActor.PersistedBits]
        msg.persistedBits.size shouldBe 0
      }

      indexerPerformerActor.underlyingActor.toRetryOperations.size shouldBe 1
      testSupervisor.underlyingActor.exceptionsCaught.size shouldBe 1
    }

    "retry and fail in case of delete error" in {
      val deleteOperation =
        Map(UUID.randomUUID().toString -> DeleteShardRecordOperation(namespace, errorLocation, bit))

      probe.send(indexerPerformerActor, PerformShardWrites(deleteOperation))
      awaitAssert {
        probe.expectMsgType[Refresh]
      }

      awaitAssert {
        val msg = localCommitLogCoordinator.expectMsgType[MetricPerformerActor.PersistedBits]
        msg.persistedBits.size shouldBe 0
      }

      indexerPerformerActor.underlyingActor.toRetryOperations.size shouldBe 1
      testSupervisor.underlyingActor.exceptionsCaught.size shouldBe 1
    }

    "retry amd eventually persist in case of unstable locations" in {
      val writeOperation =
        Map(UUID.randomUUID().toString -> WriteShardOperation(namespace, unstableRecoverableLocation, bit))

      probe.send(indexerPerformerActor, PerformShardWrites(writeOperation))
      awaitAssert {
        probe.expectMsgType[Refresh]
      }

      awaitAssert {
        val msg = localCommitLogCoordinator.expectMsgType[MetricPerformerActor.PersistedBits]
        msg.persistedBits.size shouldBe 1
      }

      indexerPerformerActor.underlyingActor.toRetryOperations.size shouldBe 0
      testSupervisor.underlyingActor.exceptionsCaught.size shouldBe 0
    }
  }
}
