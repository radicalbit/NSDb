package io.radicalbit.nsdb.cluster.actor
import akka.actor.{ActorContext, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import io.radicalbit.nsdb.cluster.PubSubTopics.NODE_GUARDIANS_TOPIC
import io.radicalbit.nsdb.common.protocol.NSDbNode
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.NodeAlive

abstract class NodeActorGuardianForTest(nodeIdOpt: Option[String]) extends NodeActorsGuardian{

  override protected lazy val nodeFsId: String = nodeIdOpt.getOrElse(nodeAddress)

  override def shutdownBehaviour(context: ActorContext, child: ActorRef) : Unit = context.stop(child)

  override def createClusterListener: ActorRef = context.actorOf(ClusterListenerTestActor.props(nodeFsId), name = "clusterListener")

}

class NodeActorGuardianFixedNamesForTest(nodeIdOpt: Option[String], volatileId: String) extends NodeActorGuardianForTest(nodeIdOpt){

  override def updateVolatileId(volatileId: String): Unit = {
    import context.dispatcher
    heartBeatDispatcher = context.system.scheduler.schedule(heartbeatInterval, heartbeatInterval) {
      mediator ! Publish(NODE_GUARDIANS_TOPIC, NodeAlive(node))
    }
  }

  node = NSDbNode(nodeAddress, nodeFsId, volatileId)
}

object NodeActorGuardianFixedNamesForTest {
  def props(volatileId: String): Props = Props(new NodeActorGuardianFixedNamesForTest(None, volatileId))
  def props(nodeId: String, volatileId: String): Props = Props(new NodeActorGuardianFixedNamesForTest(Some(nodeId), volatileId))
}


class NodeActorGuardianForTestDynamicNames(nodeIdOpt: Option[String]) extends NodeActorsGuardian{

  override protected lazy val nodeFsId: String = nodeIdOpt.getOrElse(nodeAddress)

  override def shutdownBehaviour(context: ActorContext, child: ActorRef) : Unit = context.stop(child)

  override def createClusterListener: ActorRef = context.actorOf(ClusterListenerTestActor.props(nodeFsId), name = "clusterListener")

}

object NodeActorGuardianForTestDynamicNames {
  def props(nodeId: String): Props = Props(new NodeActorGuardianForTestDynamicNames(Some(nodeId)))
}