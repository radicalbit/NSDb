package io.radicalbit.nsdb.actors

import java.nio.file.{Files, Paths}
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.IndexAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.IndexPerformerActor.PerformWrites
import io.radicalbit.nsdb.common.protocol.Bit
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class IndexerPerformerActorSpec
    extends TestKit(ActorSystem("IndexerActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter {

  val probe      = TestProbe()
  val probeActor = probe.ref

  val basePath  = "target/test_index"
  val db        = "db"
  val namespace = "namespace"
  val indexerPerformerActor =
    TestActorRef[IndexPerformerActor](IndexPerformerActor.props(basePath, db, namespace), probeActor)

  before {
    import scala.collection.JavaConverters._
    if (Paths.get(basePath, db).toFile.exists())
      Files.walk(Paths.get(basePath, db)).iterator().asScala.map(_.toFile).toSeq.reverse.foreach(_.delete)
  }

  "indexerPerformerActor" should "write and delete properly" in within(5.seconds) {

    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))

    val operations =
      Map(UUID.randomUUID().toString -> WriteOperation(namespace, "IndexerPerformerActorMetric", bit))

    probe.send(indexerPerformerActor, PerformWrites(operations))
    awaitAssert {
      probe.expectMsgType[Refresh]
    }
  }
}
