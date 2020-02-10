package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Props}
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.cluster.{Cluster, Member}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{AddLocations, RemoveNodeMetadata}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{
  AddLocationsFailed,
  LocationsAdded,
  NodeMetadataRemoved,
  RemoveNodeMetadataFailed
}
import io.radicalbit.nsdb.model.{Location, LocationWithCoordinates}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  CommitLogCoordinatorUnSubscribed,
  MetricsDataActorUnSubscribed,
  PublisherUnSubscribed
}
import io.radicalbit.rtsae.STMultiNodeSpec

import scala.concurrent.duration._

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
  |}
  |""".stripMargin))

}

class ClusterListenerSpecMultiJvmNode1 extends ClusterListenerSpec
class ClusterListenerSpecMultiJvmNode2 extends ClusterListenerSpec

sealed trait TestType
case object SuccessTest extends TestType
case object FailureTest extends TestType

class MetaDataCoordinatorForTest extends Actor with ActorLogging {
  def receive: Receive = {
    case AddLocations("success", namespace, locations) =>
      sender() ! LocationsAdded("success", namespace, locations)
    case AddLocations("failure", namespace, locations) =>
      sender() ! AddLocationsFailed("failure", namespace, locations)
    case UnsubscribeMetricsDataActor(nodeName)     => sender() ! MetricsDataActorUnSubscribed(nodeName)
    case UnSubscribeCommitLogCoordinator(nodeName) => sender() ! CommitLogCoordinatorUnSubscribed(nodeName)
    case RemoveNodeMetadata(nodeName)              => sender() ! RemoveNodeMetadataFailed(nodeName)
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

class NodeActorsGuardianForTest extends Actor with ActorLogging {
  private lazy val metaDataCoordinator = context.actorOf(Props(new MetaDataCoordinatorForTest))
  private lazy val writeCoordinator    = context.actorOf(Props(new WriteCoordinatorForTest))
  private lazy val readCoordinator     = context.actorOf(Props(new ReadCoordinatorForTest))

  def receive: Receive = {
    case GetNodeChildActors =>
      sender() ! NodeChildActorsGot(metaDataCoordinator, writeCoordinator, readCoordinator, ActorRef.noSender)
  }
}

class ClusterListenerForTest(resultActor: ActorRef, testType: TestType)
    extends ClusterListener(false) {

  val nodeActorsGuardianForTest =
    context.actorOf(Props(new NodeActorsGuardianForTest))

  override val defaultTimeout = Timeout(1.seconds)

  override def receive: Receive = super.receive orElse {
    case SubscribeAck(subscribe) => log.info("subscribe {}", subscribe)
  }
  override def createNodeActorsGuardian(): ActorRef = nodeActorsGuardianForTest

  override def retrieveLocationsToAdd(): List[LocationWithCoordinates] = testType match {
    case SuccessTest =>
      List(LocationWithCoordinates("success", "namespace", Location("metric", "node", 0L, 1L)))
    case FailureTest =>
      List(LocationWithCoordinates("failure", "namespace", Location("metric", "node", 0L, 1L)))
  }

  override def onSuccessBehaviour(readCoordinator: ActorRef,
                                  writeCoordinator: ActorRef,
                                  metadataCoordinator: ActorRef,
                                  publisherActor: ActorRef): Unit = {
    resultActor ! "Success"
  }

  override protected def onFailureBehaviour(member: Member, error: Any): Unit = {
    resultActor ! "Failure"
  }

  override protected def onRemoveNodeMetadataResponse: events.RemoveNodeMetadataResponse => Unit = {
    case NodeMetadataRemoved(_)      => //ignore
    case RemoveNodeMetadataFailed(_) => resultActor ! "Failure"
  }
}

class ClusterListenerSpec extends MultiNodeSpec(ClusterListenerSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import ClusterListenerSpecConfig._

  def initialParticipants: Int = roles.size

  private val cluster = Cluster(system)

  "ClusterListener" must {
    "successfully create a NsdbNodeEndpoint when a new member in the cluster is Up" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new ClusterListenerForTest(resultActor.testActor, SuccessTest)))
      cluster.join(node(node1).address)
      cluster.join(node(node2).address)
      enterBarrier(5 seconds, "nodes joined")
      awaitAssert(resultActor.expectMsg("Success"))
    }

    "return a failure and leave the cluster" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new ClusterListenerForTest(resultActor.testActor, FailureTest)))
      cluster.join(node(node1).address)
      cluster.join(node(node2).address)
      enterBarrier(5 seconds, "nodes joined")
      awaitAssert(resultActor.expectMsg("Failure"))
    }

    "correctly handle 'UnreachableMember' msg" in {
      val resultActor = TestProbe("resultActor")
      val clusterListener =
        cluster.system.actorOf(Props(new ClusterListenerForTest(resultActor.testActor, FailureTest)),
                               name = "clusterListener")
      awaitAssert { clusterListener ! UnreachableMember(cluster.selfMember); resultActor.expectMsg("Failure") }
    }
  }

}
