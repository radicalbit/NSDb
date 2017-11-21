package io.radicalbit.nsdb.cluster.actor

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
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
  val shardActor = system.actorOf(ShardActor.props(basePath, db, namespace))

  before {
    import scala.collection.JavaConverters._
    if (Paths.get(basePath, db).toFile.exists())
      Files.walk(Paths.get(basePath, db)).iterator().asScala.map(_.toFile).toSeq.reverse.foreach(_.delete)
  }

  "ShardActor" should "not handle non location aware messages" in {
    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    probe.send(shardActor, AddRecord(db, namespace, "shardActorMetric", bit))
    expectNoMsg(5 seconds)

    probe.send(shardActor, DeleteRecord(db, namespace, "shardActorMetric", bit))
    expectNoMsg(5 seconds)
  }

  "ShardActor" should "write and delete properly" in {

    val bit      = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))
    val location = Location("shardActorMetric", "node1", 0, 100, 0)

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

  "ShardActor" should "write and delete properly in multiple locations" in {

    val location  = Location("shardActorMetric", "node1", 0, 100, 0)
    val location2 = Location("shardActorMetric", "node1", 100, 200, 100)

    val bit = Bit(System.currentTimeMillis, 22.5, Map("content" -> "content"))

    probe.send(shardActor, AddRecordToLocation(db, namespace, "shardActorMetric2", bit, location))
    within(5 seconds) {
      probe.expectMsgType[RecordAdded]
    }

    waitInterval

    probe.send(shardActor, GetCount(db, namespace, "shardActorMetric"))
    within(5 seconds) {
      val expectedCount = probe.expectMsgType[CountGot]
      expectedCount.metric shouldBe "shardActorMetric"
      expectedCount.count shouldBe 0
    }

    probe.send(shardActor, GetCount(db, namespace, "shardActorMetric2"))
    within(5 seconds) {
      val expectedCount2 = probe.expectMsgType[CountGot]
      expectedCount2.metric shouldBe "shardActorMetric2"
      expectedCount2.count shouldBe 1
    }

  }

}
