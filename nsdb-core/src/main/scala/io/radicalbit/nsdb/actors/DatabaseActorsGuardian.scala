package io.radicalbit.actors

import akka.actor.{Actor, Props}
import io.radicalbit.actors.DatabaseActorsGuardian.GetWriteCoordinator
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.coordinator.WriteCoordinator
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.index.IndexerActor
import io.radicalbit.nsdb.metadata.MetadataService

object DatabaseActorsGuardian {

  def props = Props(new DatabaseActorsGuardian)

  sealed trait DatabaseActorsGuardianProtocol

  case object GetWriteCoordinator
}

class DatabaseActorsGuardian extends Actor {

  val config = context.system.settings.config

  val indexBasePath = config.getString("radicaldb.index.base-path")

  val metadataService  = context.actorOf(MetadataService.props, "metadata-service")
  val commitLogService = context.actorOf(CommitLogService.props, "commit-log-service")
  val indexerActor     = context.actorOf(IndexerActor.props(indexBasePath), "indexer-service")
  val readCoordinator =
    context.actorOf(ReadCoordinator.props(indexerActor), "read-coordinator")
  val writeCoordinator =
    context.actorOf(WriteCoordinator.props(indexBasePath, commitLogService, indexerActor), "write-coordinator")

  def receive = {
    case GetWriteCoordinator => sender() ! writeCoordinator
  }

}
