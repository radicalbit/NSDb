/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeoutException

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.cluster.coordinator._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._

/**
  * Actor that creates all the global singleton actors (e.g. coordinators)
  */
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

  private val metricsSchemaActor = context.actorOf(MetricsSchemaActor.props(indexBasePath), "schema-actor")

  private val metadataCache = context.actorOf(Props[ReplicatedMetadataCache], "metadata-cache")

  private val metadataCoordinator =
    context.actorOf(MetadataCoordinator.props(metadataCache), name = "metadata-coordinator")

  private val readCoordinator =
    context.actorOf(ReadCoordinator
                      .props(metadataCoordinator, metricsSchemaActor)
                      .withDispatcher("akka.actor.control-aware-dispatcher"),
                    "read-coordinator")
  private val publisherActor =
    context.actorOf(PublisherActor.props(readCoordinator).withDispatcher("akka.actor.control-aware-dispatcher"),
                    "publisher-actor")
  private val writeCoordinator =
    if (writeToCommitLog) {
      val commitLogger = context.actorOf(CommitLogCoordinator.props(), "commit-logger-coordinator")
      context.actorOf(
        WriteCoordinator
          .props(Some(commitLogger), metadataCoordinator, metricsSchemaActor, publisherActor)
          .withDispatcher("akka.actor.control-aware-dispatcher"),
        "write-coordinator"
      )
    } else
      context.actorOf(WriteCoordinator.props(None, metadataCoordinator, metricsSchemaActor, publisherActor),
                      "write-coordinator")

  context.actorOf(
    ClusterListener.props(readCoordinator = readCoordinator,
                          writeCoordinator = writeCoordinator,
                          metadataCoordinator = metadataCoordinator),
    name = "clusterListener"
  )

  def receive: Receive = {
    case GetReadCoordinator  => sender() ! readCoordinator
    case GetWriteCoordinator => sender() ! writeCoordinator
    case GetPublisher        => sender() ! publisherActor
  }

}
