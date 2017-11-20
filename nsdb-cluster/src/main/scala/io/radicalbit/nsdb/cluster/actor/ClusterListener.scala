package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Deploy}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.remote.RemoteScope
import akka.util.Timeout

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class ClusterListener extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  val mediator = DistributedPubSub(context.system).mediator

  val config = context.system.settings.config

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member) =>
      log.debug("Member is Up: {}", member.address)

      implicit val timeout: Timeout = Timeout(5 seconds)

      implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

      context.system.actorSelection("/user/guardian/metadata-coordinator").resolveOne().onComplete {
        case Success(metadataCoordinator: ActorRef) =>
          val indexBasePath = s"${config.getString("nsdb.index.base-path")}_${member.address.port.getOrElse(2552)}"

          val metadataActor = context.system.actorOf(
            MetadataActor
              .props(indexBasePath, metadataCoordinator)
              .withDeploy(Deploy(scope = RemoteScope(member.address))),
            name = s"metadata_${member.address.host.getOrElse("noHost")}_${member.address.port.getOrElse(2552)}"
          )

          mediator ! Subscribe("metadata", metadataActor)
        case Failure(exception) =>
          log.error(exception, "cannot find metadataCoordinator")
          cluster.leave(member.address)
      }

    case UnreachableMember(member) =>
      log.debug("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.debug("Member is Removed: {} after {}", member.address, previousStatus)
    case _: MemberEvent => // ignore
  }
}
