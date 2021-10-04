package io.radicalbit.nsdb.cluster.actor
import akka.actor.{ActorContext, ActorRef}

class NodeActorGuardianForTest extends NodeActorsGuardian {

  override protected lazy val nodeFsId: String = selfNodeName

  override def shutdownBehaviour(context: ActorContext, child: ActorRef) : Unit = context.stop(child)

  override def createClusterListener: ActorRef = context.actorOf(ClusterListenerTestActor.props(), name = "clusterListener")

}
