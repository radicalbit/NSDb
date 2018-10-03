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

import akka.actor.SupervisorStrategy.Resume
import akka.actor._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.MetricPerformerActor.PerformShardWrites
import io.radicalbit.nsdb.common.exception.TooManyRetriesException
import io.radicalbit.nsdb.common.protocol.{Bit, Coordinates}
import io.radicalbit.nsdb.index.BrokenTimeSeriesIndex
import io.radicalbit.nsdb.model.Location
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class TestSupervisorActor(probe: ActorRef) extends Actor with ActorLogging {

  val exceptionsCaught: ListBuffer[TooManyRetriesException] = ListBuffer.empty

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1) {
    case e: TooManyRetriesException =>
      log.error(e, "TooManyRetriesException {}", e.getMessage)
      exceptionsCaught += e
      Resume
    case t =>
      log.error(t, "generic exception")
      super.supervisorStrategy.decider.apply(t)
  }

  override def receive: Receive = {
    case msg => probe ! msg
  }
}

class MetricPerformerActorSpec
    extends TestKit(ActorSystem("IndexerActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val basePath                  = "target/test_index"
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

  val errorLocation = Location(Coordinates(db, namespace, "IndexerPerformerActorMetric"), "node1", 1, 1)
  val bit           = Bit(System.currentTimeMillis, 25, Map("content" -> "content"), Map.empty)

  override def beforeAll: Unit = {
    import scala.collection.JavaConverters._
    if (Paths.get(basePath, db).toFile.exists())
      Files.walk(Paths.get(basePath, db)).iterator().asScala.map(_.toFile).toSeq.reverse.foreach(_.delete)

    val directory =
      new MMapDirectory(
        Paths
          .get(basePath,
               db,
               namespace,
               "shards",
               s"${errorLocation.coordinates.metric}_${errorLocation.from}_${errorLocation.to}"))

    indexerPerformerActor.underlyingActor.shards += (errorLocation -> new BrokenTimeSeriesIndex(directory))
  }

  "ShardPerformerActor" should "write and delete properly" in within(5.seconds) {

    val loc = Location(Coordinates(db, namespace, "IndexerPerformerActorMetric"), "node1", 0, 0)

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

  "ShardPerformerActor" should "retry in case of write error" in within(5.seconds) {

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

    testSupervisor.underlyingActor.exceptionsCaught.size shouldBe 1
  }

  "ShardPerformerActor" should "retry in case of delete error" in within(5.seconds) {
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

    testSupervisor.underlyingActor.exceptionsCaught.size shouldBe 2
  }
}
