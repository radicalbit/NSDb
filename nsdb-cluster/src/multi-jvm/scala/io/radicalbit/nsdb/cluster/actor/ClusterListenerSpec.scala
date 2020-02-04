package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.cluster.{Cluster, Member}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{AddLocationsFailed, LocationsAdded}
import io.radicalbit.nsdb.model.{Location, LocationWithCoordinates}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetNodeChildActors, NodeChildActorsGot}
import io.radicalbit.rtsae.STMultiNodeSpec

import scala.concurrent.duration._

object ClusterListenerSpecConfig extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
  |akka.loglevel = ERROR
  |akka.actor.provider = "cluster"
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
    case _ =>
      log.warning("Unhandled message on purpose")
  }
}

class NodeActorsGuardianForTest(implicit system: ActorSystem) extends Actor with ActorLogging {
  private lazy val metaDataCoordinator = context.actorOf(Props(new MetaDataCoordinatorForTest))
  private lazy val writeCoordinator    = TestProbe("writeCoordinator").testActor
  private lazy val readCoordinator     = TestProbe("readCoordinator").testActor
  private lazy val publisherActor      = TestProbe("publisherActor").testActor

  def receive: Receive = {
    case GetNodeChildActors =>
      sender() ! NodeChildActorsGot(metaDataCoordinator, writeCoordinator, readCoordinator, publisherActor)
  }
}

class ClusterListenerForTest(resultActor: ActorRef, testType: TestType)(implicit system: ActorSystem)
    extends ClusterListener {

  override val defaultTimeout = Timeout(1.seconds)

  override def receive: Receive = super.receive orElse {
    case SubscribeAck(subscribe) => log.info("subscribe {}", subscribe)
  }
  override def createNodeActorsGuardian(): ActorRef = context.actorOf(Props(new NodeActorsGuardianForTest))

  override def retrieveLocationsToAdd(): List[LocationWithCoordinates] = testType match {
    case SuccessTest =>
      List(LocationWithCoordinates("success", "namespace", Location("metric", "node", 0L, 1L)))
    case FailureTest =>
      List(LocationWithCoordinates("failure", "namespace", Location("metric", "node", 0L, 1L)))
  }

  override def onSuccessBehaviour(readCoordinator: ActorRef, writeCoordinator: ActorRef, metadataCoordinator: ActorRef, publisherActor: ActorRef): Unit = {
    resultActor ! "Success"
  }

  override protected def onFailureBehaviour(member: Member, error: Any): Unit = {
    resultActor ! "Failure"
  }
}

class ClusterListenerSpec extends MultiNodeSpec(ClusterListenerSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import ClusterListenerSpecConfig._

  def initialParticipants: Int = roles.size

  val cluster = Cluster(system)

  "ClusterListener" must {
    "successfully create a NsdbNodeEndpoint when a new member in the cluster is Up" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new ClusterListenerForTest(resultActor.testActor, SuccessTest)))
      cluster.join(node(node1).address)
      cluster.join(node(node2).address)
      enterBarrier(5 seconds, "1")
      resultActor.expectMsg("Success")
    }

    "return a failure and leave the cluster" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new ClusterListenerForTest(resultActor.testActor, FailureTest)))
      cluster.join(node(node1).address)
      cluster.join(node(node2).address)
      enterBarrier(5 seconds, "1")
      resultActor.expectMsg(15 seconds,"Failure")
    }
  }

}
