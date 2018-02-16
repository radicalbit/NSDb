package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeoutException

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.cluster.coordinator.{CommitLogCoordinator, ReadCoordinator, WriteCoordinator}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._

object DatabaseActorsGuardian {
  def props = Props(new DatabaseActorsGuardian)
}

class DatabaseActorsGuardian extends Actor with ActorLogging {

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: TimeoutException =>
      log.error("Got the following TimeoutException, resuming the processing", e)
      Resume
    case t =>
      log.error("generic error in write coordinator", t)
      super.supervisorStrategy.decider.apply(t)
  }

  private val config = context.system.settings.config

  private val indexBasePath = config.getString("nsdb.index.base-path")

  private val writeToCommitLog = config.getBoolean("nsdb.commit-log.enabled")

  private val namespaceSchemaActor = context.actorOf(NamespaceSchemaActor.props(indexBasePath), "schema-actor")

  val metadataCache = context.actorOf(Props[ReplicatedMetadataCache], "metadata-cache")

  val metadataCoordinator = context.actorOf(MetadataCoordinator.props(metadataCache), name = "metadata-coordinator")

  private val commitLogCoordinator =
    if (writeToCommitLog) Some(context.actorOf(CommitLogCoordinator.props, "commit-log-coordinator")) else None
  private val readCoordinator =
    context.actorOf(ReadCoordinator.props(metadataCoordinator, namespaceSchemaActor), "read-coordinator")
  private val publisherActor =
    context.actorOf(PublisherActor.props(readCoordinator, namespaceSchemaActor), "publisher-actor")
  private val writeCoordinator =
    context.actorOf(
      WriteCoordinator.props(metadataCoordinator, namespaceSchemaActor, commitLogCoordinator, publisherActor),
      "write-coordinator")

  context.actorOf(
    ClusterListener.props(readCoordinator = readCoordinator,
                          writeCoordinator = writeCoordinator,
                          metadataCoordinator = metadataCoordinator,
                            commitLogCoordinator = commitLogCoordinator),
    name = "clusterListener"
  )

  def receive: Receive = {
    case GetReadCoordinator  => sender() ! readCoordinator
    case GetWriteCoordinator => sender() ! writeCoordinator
    case GetPublisher        => sender() ! publisherActor
  }

}
