package io.radicalbit.nsdb.cluster.actor

import akka.actor.{ActorRef, Props}
import akka.cluster.Member
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import io.radicalbit.nsdb.cluster.actor.ClusterListenerTestActor._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{NodeMetadataRemoved, RemoveNodeMetadataFailed}
import io.radicalbit.nsdb.common.protocol.NSDbNode
import io.radicalbit.nsdb.model.{Location, LocationWithCoordinates}

abstract class ClusterListenerTestActor
  extends AbstractClusterListener {

  def resultActor: ActorRef = ActorRef.noSender
  def testType: TestType = SuccessTest

  override def enableClusterMetricsExtension: Boolean = false

  override protected lazy val nodeId: String = selfNodeName

  val node = NSDbNode("nodeAddress", nodeId, "1")

  override def receive: Receive = super.receive orElse {
    case SubscribeAck(subscribe) => log.info("subscribe {}", subscribe)
  }

  override def retrieveLocationsToAdd(node: NSDbNode): List[LocationWithCoordinates] = testType match {
    case SuccessTest =>
      List.empty
    case FailureTest =>
      List(LocationWithCoordinates("failure", "namespace", Location("metric", node, 0L, 1L)))
  }

  override def onSuccessBehaviour(readCoordinator: ActorRef,
                                  writeCoordinator: ActorRef,
                                  metadataCoordinator: ActorRef,
                                  publisherActor: ActorRef): Unit = ()

  override protected def onFailureBehaviour(member: Member, error: Any): Unit = ()

  override protected def onRemoveNodeMetadataResponse: events.RemoveNodeMetadataResponse => Unit = {
    case NodeMetadataRemoved(_)      => //ignore
    case RemoveNodeMetadataFailed(_) => resultActor ! "Failure"
  }
}

object ClusterListenerTestActor {

  def props(): Props = Props(new ClusterListenerTestActor{})

  sealed trait TestType
  case object SuccessTest extends TestType
  case object FailureTest extends TestType
}