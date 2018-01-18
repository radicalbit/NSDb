package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeoutException

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian._
import io.radicalbit.nsdb.cluster.coordinator.{ReadCoordinator, WriteCoordinator}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.NamespaceDataActorSubscribed

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object DatabaseActorsGuardian {

  case object GetMetadataCoordinator

  def props = Props(new DatabaseActorsGuardian)

}

class DatabaseActorsGuardian extends Actor with ActorLogging {

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: TimeoutException =>
      context.system.log.error("Got the following TimeoutException, resuming the processing", e)
      Resume
    case t => super.supervisorStrategy.decider.apply(t)
  }

  private val config = context.system.settings.config

  private val indexBasePath = config.getString("nsdb.index.base-path")

  private val writeToCommitLog = config.getBoolean("nsdb.commit-log.enabled")

  private val commitLogService =
    if (writeToCommitLog) Some(context.actorOf(CommitLogService.props, "commit-log-service")) else None

  private val namespaceSchemaActor = context.actorOf(NamespaceSchemaActor.props(indexBasePath), "schema-actor")

  private val namespaceDataActor = context.actorOf(NamespaceDataActor.props(indexBasePath), "namespace-data-actor")

  val metadataCache = context.actorOf(Props[ReplicatedMetadataCache], "metadata-cache")

  val metadataCoordinator = context.actorOf(MetadataCoordinator.props(metadataCache), name = "metadata-coordinator")

  private val readCoordinator =
    context.actorOf(ReadCoordinator.props(metadataCoordinator, namespaceSchemaActor), "read-coordinator")
  private val publisherActor =
    context.actorOf(PublisherActor.props(readCoordinator, namespaceSchemaActor), "publisher-actor")
  private val writeCoordinator =
    context.actorOf(
      WriteCoordinator.props(metadataCoordinator, namespaceSchemaActor, commitLogService, publisherActor),
      "write-coordinator")

  context.actorOf(
    ClusterListener.props(readCoordinator = readCoordinator,
                          writeCoordinator = writeCoordinator,
                          metadataCoordinator = metadataCoordinator),
    name = "clusterListener"
  )

  if (!context.system.settings.config.getBoolean("nsdb.sharding.enabled")) {
    implicit val timeout = Timeout(5 seconds)
    import context.dispatcher

    (writeCoordinator ? SubscribeNamespaceDataActor(namespaceDataActor)).onComplete {
      case Success(msg: NamespaceDataActorSubscribed) => log.info("WriteCoordinator is ready")
      case Success(msg) =>
        log.error(s"unexepted response $msg from WriteCoordinator. need to quit")
        System.exit(1)
      case Failure(t) =>
        log.error(s"error when starting WriteCoordinator", t)
        System.exit(1)
    }

    (readCoordinator ? SubscribeNamespaceDataActor(namespaceDataActor)).onComplete {
      case Success(msg: NamespaceDataActorSubscribed) => log.info("ReadCoordinator is ready")
      case Success(msg) =>
        log.error(s"unexepted response $msg from ReadCoordinator. need to quit")
        System.exit(1)
      case Failure(t) =>
        log.error(s"error when starting ReadCoordinator", t)
        System.exit(1)
    }
  }

  def receive: Receive = {
    case GetReadCoordinator     => sender() ! readCoordinator
    case GetWriteCoordinator    => sender() ! writeCoordinator
    case GetMetadataCoordinator => sender() ! metadataCoordinator
    case GetPublisher           => sender() ! publisherActor
  }

}
