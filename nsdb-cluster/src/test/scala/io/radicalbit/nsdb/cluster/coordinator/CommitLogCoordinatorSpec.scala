//package io.radicalbit.nsdb.cluster.coordinator
//
//import akka.actor.{ActorSystem, Props}
//import akka.pattern.ask
//import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
//import akka.util.Timeout
//import io.radicalbit.nsdb.cluster.coordinator.CommitLogCoordinator.{Insert, SubscribeWriter, WriteToCommitLogSucceeded}
//import io.radicalbit.nsdb.commit_log.{
//  CommitLogEntry,
//  CommitLogSerializer,
//  CommitLogWriterActor,
//  KryoCommitLogSerializer
//}
//import io.radicalbit.nsdb.common.protocol.Bit
//import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
//
//import scala.collection.mutable
//import scala.concurrent.Await
//import scala.concurrent.duration._
//
//class MemoryWriter extends CommitLogWriterActor {
//
//  val log: mutable.Seq[Array[Byte]] = mutable.Seq.empty
//
//  override protected def serializer: CommitLogSerializer = new KryoCommitLogSerializer
//
//  override protected def createEntry(commitLogEntry: CommitLogEntry): Unit =
//    log :+ serializer.serialize(commitLogEntry)
//}
//
//class CommitLogCoordinatorSpec
//    extends TestKit(ActorSystem("CommitLogCoordinatorSpec"))
//    with ImplicitSender
//    with WordSpecLike
//    with Matchers
//    with BeforeAndAfterAll {
//
//  private val writer1                   = TestActorRef[MemoryWriter](Props[MemoryWriter])
//  private val writer2                   = TestActorRef[MemoryWriter](Props[MemoryWriter])
//  private val commitLogCoordinatorActor = system actorOf CommitLogCoordinator.props
//
//  override def beforeAll = {
//    implicit val timeout: Timeout = Timeout(5.seconds)
//
//    Await.result(commitLogCoordinatorActor ? SubscribeWriter("testNode", writer1), 3.seconds)
//    Await.result(commitLogCoordinatorActor ? SubscribeWriter("testNode1", writer2), 3.seconds)
//  }
//
//  "CommitLogCoordinator" should {
//    "write a insert entry and commit it" in within(5.seconds) {
//
//      awaitAssert {
//        commitLogCoordinatorActor ! Insert("testMetric", Bit(0, 1, Map("dim" -> "v")))
//        expectMsgType[WriteToCommitLogSucceeded]
//        writer1.underlyingActor.log.size shouldBe 1
//        writer2.underlyingActor.log.size shouldBe 1
//      }
//    }
//
//    "write a insert entry and reject it" in {}
//
//    "write a delete entry and commit it" in {}
////    "write a delete entry and commit it" in {}
//  }
//
//}
