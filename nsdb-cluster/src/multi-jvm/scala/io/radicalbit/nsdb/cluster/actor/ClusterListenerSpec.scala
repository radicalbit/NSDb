package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.Member
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.ClusterListenerTestActor.{FailureTest, SuccessTest, TestType}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{AddLocations, GetOutdatedLocations, RemoveNodeMetadata}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{AddLocationsFailed, LocationsAdded, OutdatedLocationsGot, RemoveNodeMetadataFailed}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{CommitLogCoordinatorUnSubscribed, MetricsDataActorUnSubscribed, PublisherUnSubscribed}
import io.radicalbit.nsdb.STMultiNodeSpec

import scala.concurrent.duration._

class ClusterListenerSpecMultiJvmNode1 extends ClusterListenerSpec
class ClusterListenerSpecMultiJvmNode2 extends ClusterListenerSpec

class MetaDataCoordinatorForTest extends Actor with ActorLogging {
  def receive: Receive = {
    case AddLocations("success", namespace, locations, _) =>
      sender() ! LocationsAdded("success", namespace, locations)
    case AddLocations("failure", namespace, locations, _) =>
      sender() ! AddLocationsFailed("failure", namespace, locations)
    case UnsubscribeMetricsDataActor(nodeName)     => sender() ! MetricsDataActorUnSubscribed(nodeName)
    case UnSubscribeCommitLogCoordinator(nodeName) => sender() ! CommitLogCoordinatorUnSubscribed(nodeName)
    case RemoveNodeMetadata(nodeName)              => sender() ! RemoveNodeMetadataFailed(nodeName)
    case GetOutdatedLocations                      => sender() ! OutdatedLocationsGot(Seq.empty)
    case _ =>
      log.warning("Unhandled message on purpose")
  }
}

class ReadCoordinatorForTest extends Actor with ActorLogging {
  def receive: Receive = {
    case UnsubscribeMetricsDataActor(nodeName) => sender() ! MetricsDataActorUnSubscribed(nodeName)
  }
}

class WriteCoordinatorForTest extends Actor with ActorLogging {
  def receive: Receive = {
    case UnSubscribeCommitLogCoordinator(nodeName) => sender() ! CommitLogCoordinatorUnSubscribed(nodeName)
    case UnSubscribePublisher(nodeName)            => sender() ! PublisherUnSubscribed(nodeName)
    case UnsubscribeMetricsDataActor(nodeName)     => sender() ! MetricsDataActorUnSubscribed(nodeName)
  }
}

class NodeActorsGuardianForTest(val resultActor: ActorRef, val testType: TestType) extends Actor with ActorLogging {

  context.actorOf(Props(new ClusterListenerWithMockedChildren(resultActor, testType)))

  private lazy val metaDataCoordinator = context.actorOf(Props(new MetaDataCoordinatorForTest))
  private lazy val writeCoordinator    = context.actorOf(Props(new WriteCoordinatorForTest))
  private lazy val readCoordinator     = context.actorOf(Props(new ReadCoordinatorForTest))

  def receive: Receive = {
    case GetNodeChildActors =>
      sender() ! NodeChildActorsGot(metaDataCoordinator, writeCoordinator, readCoordinator, ActorRef.noSender)
  }
}

class ClusterListenerWithMockedChildren(override val resultActor: ActorRef, override val testType: TestType) extends ClusterListenerTestActor {
//  val nodeActorsGuardianForTest =
//          context.actorOf(Props(new NodeActorsGuardianForTest), name = s"guardian_${selfNodeName}")

//          override def createNodeActorsGuardian(): ActorRef = nodeActorsGuardianForTest

  override def onSuccessBehaviour(readCoordinator: ActorRef,
                                  writeCoordinator: ActorRef,
                                  metadataCoordinator: ActorRef,
                                  publisherActor: ActorRef): Unit = {
        resultActor ! "Success"
  }

  override protected def onFailureBehaviour(member: Member, error: Any): Unit = {
        resultActor ! "Failure"
  }
}

object ClusterListenerSpecConfig extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
                                           |akka.loglevel = ERROR
                                           |akka.actor.provider = "cluster"
                                           |nsdb {
                                           | retry-policy {
                                           |    delay = 1 second
                                           |    n-retries = 2
                                           |  }
                                           |  heartbeat.interval = 10 seconds
                                           |  global.timeout = 30 seconds
                                           |}
                                           |""".stripMargin))

}

class ClusterListenerSpec extends MultiNodeSpec(ClusterListenerSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import ClusterListenerSpecConfig._

  def initialParticipants: Int = roles.size

  "ClusterListener" must {
    "successfully create a NsdbNodeEndpoint when a new member in the cluster is Up" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new NodeActorsGuardianForTest(resultActor.testActor, SuccessTest)))
      cluster.join(node(node1).address)
      cluster.join(node(node2).address)
      enterBarrier(5 seconds, "nodes joined")
      awaitAssert(resultActor.expectMsg("Success"))
    }

    "return a failure and leave the cluster" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new NodeActorsGuardianForTest(resultActor.testActor, FailureTest)))
      cluster.join(node(node1).address)
      cluster.join(node(node2).address)
      enterBarrier(5 seconds, "nodes joined")
      awaitAssert(resultActor.expectMsg("Failure"))
    }

    "correctly handle 'UnreachableMember' msg" in {
      val resultActor = TestProbe("resultActor")
      val clusterListener =
        cluster.system.actorOf(Props(new NodeActorsGuardianForTest(resultActor.testActor, FailureTest)))
      clusterListener ! UnreachableMember(cluster.selfMember)
      awaitAssert(resultActor.expectMsg("Failure"))
    }
  }

}
