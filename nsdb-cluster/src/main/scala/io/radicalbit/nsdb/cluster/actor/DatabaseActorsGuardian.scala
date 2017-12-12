package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeoutException

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, OneForOneStrategy, Props}
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.actors.{NamespaceSchemaActor, PublisherActor}
import io.radicalbit.nsdb.cluster.coordinator.{ReadCoordinator, WriteCoordinator}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetPublisher, GetReadCoordinator, GetWriteCoordinator}

object DatabaseActorsGuardian {

  def props = Props(new DatabaseActorsGuardian)

}

class DatabaseActorsGuardian extends Actor {

  override val supervisorStrategy = OneForOneStrategy() {
    case e: TimeoutException =>
      context.system.log.error("Got the following TimeoutException, resuming the processing", e)
      Resume
    case t => super.supervisorStrategy.decider.apply(t)
  }

  val config = context.system.settings.config

  val indexBasePath = config.getString("nsdb.index.base-path")

  val writeToCommitLog = config.getBoolean("nsdb.commit-log.enabled")

  val commitLogService =
    if (writeToCommitLog) Some(context.actorOf(CommitLogService.props, "commit-log-service")) else None
  val namespaceSchemaActor = context.actorOf(NamespaceSchemaActor.props(indexBasePath), "schema-actor")
  val namespaceActor       = context.actorOf(NamespaceDataActor.props(indexBasePath), "namespace-actor")
  val readCoordinator =
    context.actorOf(ReadCoordinator.props(namespaceSchemaActor, namespaceActor), "read-coordinator")
  val publisherActor =
    context.actorOf(PublisherActor.props(indexBasePath, readCoordinator, namespaceSchemaActor), "publisher-actor")
  val writeCoordinator =
    context.actorOf(WriteCoordinator.props(namespaceSchemaActor, commitLogService, namespaceActor, publisherActor),
                    "write-coordinator")

  def receive = {
    case GetReadCoordinator  => sender() ! readCoordinator
    case GetWriteCoordinator => sender() ! writeCoordinator
    case GetPublisher        => sender() ! publisherActor
  }

}
