package io.radicalbit.nsdb.cluster.actor
import akka.actor.ActorRef

class NodeActorGuardianForTest extends NodeActorsGuardian {

  override protected lazy val nodeId: String = selfNodeName

  override def createClusterListener: ActorRef = context.actorOf(ClusterListenerTestActor.props(), name = "clusterListener")

}
