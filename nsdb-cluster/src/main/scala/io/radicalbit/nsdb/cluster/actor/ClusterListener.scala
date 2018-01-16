package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Deploy, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.remote.RemoteScope
import akka.util.Timeout
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.SubscribeNamespaceDataActor

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class ClusterListener(writeCoordinator: ActorRef, readCoordinator: ActorRef, metadataCoordinator: ActorRef)
    extends Actor
    with ActorLogging {

  val cluster = Cluster(context.system)

  val mediator = DistributedPubSub(context.system).mediator

  val config = context.system.settings.config

  lazy val sharding: Boolean = context.system.settings.config.getBoolean("nsdb.sharding.enabled")

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)

      val nameNode = s"${member.address.host.getOrElse("noHost")}_${member.address.port.getOrElse(2552)}"

      implicit val timeout: Timeout = Timeout(5 seconds)

      implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

      val indexBasePath = config.getString("nsdb.index.base-path")

      val metadataActor = context.system.actorOf(
        MetadataActor
          .props(indexBasePath, metadataCoordinator)
          .withDeploy(Deploy(scope = RemoteScope(member.address))),
        name = s"metadata_$nameNode"
      )

      mediator ! Subscribe("metadata", metadataActor)

      if (sharding) {
        log.info(s"subscribing data actor for node $nameNode")
        val namespaceActor = context.actorOf(
          NamespaceDataActor.props(indexBasePath).withDeploy(Deploy(scope = RemoteScope(member.address))),
          s"namespace-data-actor_$nameNode")
        writeCoordinator ! SubscribeNamespaceDataActor(namespaceActor, Some(nameNode))
        readCoordinator ! SubscribeNamespaceDataActor(namespaceActor, Some(nameNode))
      }

    case UnreachableMember(member) =>
      log.debug("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.debug("Member is Removed: {} after {}", member.address, previousStatus)
    case _: MemberEvent => // ignore
  }
}

object ClusterListener {
  def props(writeCoordinator: ActorRef, readCoordinator: ActorRef, metadataCoordinator: ActorRef) =
    Props(new ClusterListener(writeCoordinator, readCoordinator, metadataCoordinator))
}
