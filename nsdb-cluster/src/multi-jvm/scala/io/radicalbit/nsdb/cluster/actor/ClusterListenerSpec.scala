package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.{Cluster, Member}
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.AddLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{AddLocationsFailed, LocationsAdded}
import io.radicalbit.nsdb.model.{Location, LocationWithCoordinates}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetNodeChildActors, NodeChildActorsGot}
import io.radicalbit.rtsae.STMultiNodeSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ClusterListenerSpecConfig extends MultiNodeConfig {
  val node1 = role("node-1")
//  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
  |akka.loglevel = ERROR
  |akka.actor.provider = "cluster"
  |""".stripMargin))

}

class ClusterListenerSpecMultiJvmNode1 extends ClusterListenerSpec
//class ClusterListenerSpecMultiJvmNode2 extends ClusterListenerSpec

sealed trait TestType
case object SuccessTest extends TestType
case object FailureTest extends TestType
case object TimeoutTest extends TestType

class MetaDataCoordinatorForTest extends Actor with ActorLogging {
  def receive: Receive = {
    case AddLocations("success", namespace, locations) =>
      sender() ! LocationsAdded("success", namespace, locations)
    case AddLocations("failure", namespace, locations) =>
      sender() ! AddLocationsFailed("failure", namespace, locations)
    case _ =>
      log.debug("Unhandled message on purpose")
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

class ClusterListenerForTest(resultActor: ActorRef, testType: TestType)(implicit system: ActorSystem,
                                                                        ec: ExecutionContext)
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
    case TimeoutTest =>
      List(LocationWithCoordinates("timeout", "namespace", Location("metric", "node", 0L, 1L)))
  }

  override def handleF(f: Future[(List[LocationsAdded], List[AddLocationsFailed])],
                       readCoordinator: ActorRef,
                       writeCoordinator: ActorRef,
                       metadataCoordinator: ActorRef,
                       publisherActor: ActorRef,
                       member: Member): Unit =
    f onComplete {
      case Success((_, failures)) if failures.isEmpty => resultActor ! "Success"
      case Success((_, _))                            => resultActor ! "Failure1"
      case Failure(_)                                 => resultActor ! "Failure2"
    }
}

class ClusterListenerSpec extends MultiNodeSpec(ClusterListenerSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import ClusterListenerSpecConfig._
  import system.dispatcher

  def initialParticipants: Int = roles.size

  val cluster = Cluster(system)

  "ClusterListener" must {
    "success" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new ClusterListenerForTest(resultActor.testActor, SuccessTest)))
      cluster.join(node(node1).address)
      enterBarrier(5 seconds, "1")
      resultActor.expectMsg("Success")
    }

    "failure1" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new ClusterListenerForTest(resultActor.testActor, FailureTest)))
      cluster.join(node(node1).address)
      enterBarrier(5 seconds, "1")
      resultActor.expectMsg("Failure1")
    }

    "failure2" in {
      val resultActor = TestProbe("resultActor")
      cluster.system.actorOf(Props(new ClusterListenerForTest(resultActor.testActor, TimeoutTest)))
      cluster.join(node(node1).address)
      enterBarrier(5 seconds, "1")
      resultActor.expectMsg(10 seconds, "Failure2")
    }
  }

}
