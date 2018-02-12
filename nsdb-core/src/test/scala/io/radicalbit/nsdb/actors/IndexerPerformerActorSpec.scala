package io.radicalbit.nsdb.actors

import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
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

  val interval = FiniteDuration(system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
                                TimeUnit.SECONDS)

  val basePath  = "target/test_index"
  val db        = "db"
  val namespace = "namespace"
  val indexerPerformerActor =
    system.actorOf(IndexPerformerActor.props(basePath, db, namespace, Some(probeActor)), "indexerPerformerActorTest")

  before {
    import scala.collection.JavaConverters._
    if (Paths.get(basePath, db).toFile.exists())
      Files.walk(Paths.get(basePath, db)).iterator().asScala.map(_.toFile).toSeq.reverse.foreach(_.delete)
  }

  "indexerPerformerActor" should "write and delete properly" in {

    val bit = Bit(System.currentTimeMillis, 25, Map("content" -> "content"))

    val operations = Map(UUID.randomUUID().toString -> Seq(WriteOperation(namespace, "indexerActorMetric", bit)))

    probe.send(indexerPerformerActor, PerformWrites(operations))
    within(5.seconds) {
      probe.expectMsgType[Refresh]
    }
  }
}
