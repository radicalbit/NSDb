package io.radicalbit.actors

import akka.actor.{Actor, Props}
import io.radicalbit.actors.DatabaseActorsGuardian.GetWriteCoordinator
import io.radicalbit.commit_log.CommitLogService
import io.radicalbit.coordinator.WriteCoordinator
import io.radicalbit.index.IndexerActor
import io.radicalbit.metadata.MetadataService

object DatabaseActorsGuardian {

  def props = Props(new DatabaseActorsGuardian)

  sealed trait DatabaseActorsGuardianProtocol

  case object GetWriteCoordinator
}

class DatabaseActorsGuardian extends Actor {

  val metadataService  = context.actorOf(MetadataService.props, "metadata-service")
  val commitLogService = context.actorOf(CommitLogService.props, "commit-log-service")
  val indexerActor     = context.actorOf(IndexerActor.props("target/test_index"), "indexer-service")
  val writeCoordinator =
    context.actorOf(WriteCoordinator.props("target/test_index", commitLogService, indexerActor), "write-coordinator")

  def receive = {
    case GetWriteCoordinator => sender() ! writeCoordinator
  }

}
