package io.radicalbit.nsdb.cluster.actor

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.cluster.ClusterWriteInterval
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor.{AddRecordToLocation, DeleteRecordFromLocation}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class ShardActorSpec()
    extends TestKit(ActorSystem("shardActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter
    with ClusterWriteInterval {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val basePath   = "target/test_index"
  val db         = "db_shard"
  val namespace  = "namespace"
  val shardActor = TestActorRef[ShardActor](ShardActor.props(basePath, db, namespace))

  before {
    import scala.collection.JavaConverters._
    if (Paths.get(basePath, db).toFile.exists())
      Files.walk(Paths.get(basePath, db)).iterator().asScala.map(_.toFile).toSeq.reverse.foreach(_.delete)
  }

  "ShardActor" should "not handle non location aware messages" in {
    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    probe.send(shardActor, AddRecord(db, namespace, "shardActorMetric", bit))
    expectNoMessage(5 seconds)

    probe.send(shardActor, DeleteRecord(db, namespace, "shardActorMetric", bit))
    expectNoMessage(5 seconds)
  }

  "ShardActor" should "write and delete properly" in {

    val bit      = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    val location = Location("shardActorMetric", "node1", 0, 100)

    probe.send(shardActor, AddRecordToLocation(db, namespace, "shardActorMetric", bit, location))
    within(5 seconds) {
      val expectedAdd = probe.expectMsgType[RecordAdded]
      expectedAdd.metric shouldBe "shardActorMetric"
      expectedAdd.record shouldBe bit
    }
    waitInterval

    probe.send(shardActor, GetCount(db, namespace, "shardActorMetric"))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe "shardActorMetric"
      expectedCount.count shouldBe 1
    }
    probe.send(shardActor, DeleteRecordFromLocation(db, namespace, "shardActorMetric", bit, location))
    within(5 seconds) {
      val expectedDelete = probe.expectMsgType[RecordDeleted]
      expectedDelete.metric shouldBe "shardActorMetric"
      expectedDelete.record shouldBe bit
    }
    waitInterval

    probe.send(shardActor, GetCount(db, namespace, "shardActorMetric"))
    within(5 seconds) {
      val expectedCountDeleted = probe.expectMsgType[CountGot]
      expectedCountDeleted.metric shouldBe "shardActorMetric"
      expectedCountDeleted.count shouldBe 0
    }

  }

  "ShardActor" should "write and delete properly the same metric in multiple locations" in {

    val location  = Location("shardActorMetric", "node1", 0, 100)
    val location2 = Location("shardActorMetric", "node1", 101, 200)

    val bit11 = Bit(System.currentTimeMillis, 22.5, Map("content" -> "content"))
    val bit12 = Bit(System.currentTimeMillis, 30.5, Map("content" -> "content"))
    val bit13 = Bit(System.currentTimeMillis, 50.5, Map("content" -> "content"))
    val bit21 = Bit(System.currentTimeMillis, 150, Map("content"  -> "content"))
    val bit22 = Bit(System.currentTimeMillis, 160, Map("content"  -> "content"))

    probe.send(shardActor, AddRecordToLocation(db, namespace, "shardActorMetric", bit11, location))
    probe.send(shardActor, AddRecordToLocation(db, namespace, "shardActorMetric", bit12, location))
    probe.send(shardActor, AddRecordToLocation(db, namespace, "shardActorMetric", bit13, location))
    probe.send(shardActor, AddRecordToLocation(db, namespace, "shardActorMetric", bit21, location2))
    probe.send(shardActor, AddRecordToLocation(db, namespace, "shardActorMetric", bit22, location2))
    within(3 seconds) {
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
      probe.expectMsgType[RecordAdded]
    }

    waitInterval

    probe.send(shardActor, GetCount(db, namespace, "shardActorMetric"))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe "shardActorMetric"
      expectedCount.count shouldBe 5

      shardActor.underlyingActor.shards.size shouldBe 2
      shardActor.underlyingActor.shards.keys.toSeq.contains(ShardKey("shardActorMetric", 0, 100))
      shardActor.underlyingActor.shards.keys.toSeq.contains(ShardKey("shardActorMetric", 101, 200))

      val i1     = shardActor.underlyingActor.shards(ShardKey("shardActorMetric", 0, 100))
      val shard1 = i1.getAll()(i1.getSearcher)
      shard1.size shouldBe 3
      shard1 should contain(bit11)
      shard1 should contain(bit12)
      shard1 should contain(bit13)

      val i2     = shardActor.underlyingActor.shards(ShardKey("shardActorMetric", 101, 200))
      val shard2 = i2.getAll()(i2.getSearcher)
      shard2.size shouldBe 2
      shard2 should contain(bit21)
      shard2 should contain(bit22)
    }
  }

}
