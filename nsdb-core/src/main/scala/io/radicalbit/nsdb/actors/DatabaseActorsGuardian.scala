package io.radicalbit.nsdb.actors

import akka.actor.{Actor, Props}
import io.radicalbit.nsdb.actors.DatabaseActorsGuardian.{GetReadCoordinator, GetWriteCoordinator}
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.nsdb.coordinator.{ReadCoordinator, WriteCoordinator}
import io.radicalbit.nsdb.metadata.MetadataService

object DatabaseActorsGuardian {

  def props = Props(new DatabaseActorsGuardian)

  sealed trait DatabaseActorsGuardianProtocol

  case object GetReadCoordinator
  case object GetWriteCoordinator
}

class DatabaseActorsGuardian extends Actor {

  val config = context.system.settings.config

  val indexBasePath = config.getString("radicaldb.index.base-path")

  val metadataService  = context.actorOf(MetadataService.props, "metadata-service")
  val commitLogService = context.actorOf(CommitLogService.props, "commit-log-service")
  val schemaActor      = context.actorOf(SchemaCoordinatorActor.props(indexBasePath), "schema-actor")
  val namespaceActor   = context.actorOf(NamespaceActor.props(indexBasePath), "namespace-actor")
  val publisherActor   = context.actorOf(PublisherActor.props(indexBasePath), "publisher-actor")
  val readCoordinator =
    context.actorOf(ReadCoordinator.props(schemaActor, namespaceActor), "read-coordinator")
  val writeCoordinator =
    context.actorOf(WriteCoordinator.props(schemaActor, commitLogService, namespaceActor, publisherActor),
                    "write-coordinator")

  def receive = {
    case GetReadCoordinator  => sender() ! readCoordinator
    case GetWriteCoordinator => sender() ! writeCoordinator
  }

}
