package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging, Deploy, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.remote.RemoteScope
import io.radicalbit.nsdb.cluster.PubSubTopics.NODE_GUARDIANS_TOPIC
import io.radicalbit.nsdb.cluster.createNodeName

class ClusterListenerTestActor(nodeActorsGuardianProps: Props)  extends Actor with ActorLogging {

  private lazy val cluster             = Cluster(context.system)
  private lazy val mediator = DistributedPubSub(context.system).mediator

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def receive: Receive = {
    case MemberUp(member) if member == cluster.selfMember =>

      val nodeName = createNodeName(member)

      val nodeActorsGuardian =
        context.system.actorOf(nodeActorsGuardianProps.withDeploy(Deploy(scope = RemoteScope(member.address))),
          name = s"guardian_$nodeName")

      mediator ! Subscribe(NODE_GUARDIANS_TOPIC, nodeActorsGuardian)
  }

}

object ClusterListenerTestActor {
  def props(nodeActorsGuardianProps: Props): Props =
    Props(new ClusterListenerTestActor(nodeActorsGuardianProps))
}
