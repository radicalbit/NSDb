package io.radicalbit.nsdb.cluster.actor
import akka.actor.{ActorContext, ActorRef, Props}
import io.radicalbit.nsdb.cluster.VOLATILE_ID_LENGTH
import io.radicalbit.nsdb.common.protocol.NSDbNode
import org.apache.commons.lang3.RandomStringUtils

class NodeActorGuardianForTest(val volatileId: String) extends NodeActorsGuardian{

  override protected lazy val nodeFsId: String = selfNodeName

  override def shutdownBehaviour(context: ActorContext, child: ActorRef) : Unit = context.stop(child)

  override def createClusterListener: ActorRef = context.actorOf(ClusterListenerTestActor.props(), name = "clusterListener")

  node = NSDbNode(nodeAddress, nodeFsId, volatileId)
}

object NodeActorGuardianForTest {
  def props: Props = Props(new NodeActorGuardianForTest(RandomStringUtils.randomAlphabetic(VOLATILE_ID_LENGTH)))

  def props(volatileId: String): Props = Props(new NodeActorGuardianForTest(volatileId))
}
