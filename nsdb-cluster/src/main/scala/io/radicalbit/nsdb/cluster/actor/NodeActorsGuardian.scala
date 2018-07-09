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
import akka.actor.{Actor, ActorLogging, ActorRef, Deploy, OneForOneStrategy, Props, SupervisorStrategy}
import akka.cluster.Cluster
import akka.remote.RemoteScope
import io.radicalbit.nsdb.actors.PublisherActor
import io.radicalbit.nsdb.cluster.coordinator._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetCoordinators, _}

/**
  * Actor that creates all the node singleton actors (e.g. coordinators)
  */
class NodeActorsGuardian(metadataCache: ActorRef) extends Actor with ActorLogging {

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: TimeoutException =>
      log.error("Got the following TimeoutException, resuming the processing", e)
      Resume
    case t =>
      log.error("generic error in write coordinator", t)
      super.supervisorStrategy.decider.apply(t)
  }

  val selfMember = Cluster(context.system).selfMember

  val nodeName = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

  private val config = context.system.settings.config

  private val indexBasePath = config.getString("nsdb.index.base-path")

  private val writeToCommitLog = config.getBoolean("nsdb.commit-log.enabled")

  private val metricsSchemaActor = context.actorOf(
    MetricsSchemaActor.props(indexBasePath).withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
    s"metrics-schema-actor_$nodeName")

  private val metadataCoordinator =
    context.actorOf(
      MetadataCoordinator.props(metadataCache).withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      name = s"metadata-coordinator_$nodeName")

  private val readCoordinator =
    context.actorOf(
      ReadCoordinator
        .props(metadataCoordinator, metricsSchemaActor)
        .withDispatcher("akka.actor.control-aware-dispatcher")
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      s"read-coordinator_$nodeName"
    )
  private val publisherActor =
    context.actorOf(
      PublisherActor
        .props(readCoordinator)
        .withDispatcher("akka.actor.control-aware-dispatcher")
        .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
      s"publisher-actor_$nodeName"
    )
  private val writeCoordinator =
    if (writeToCommitLog) {
      val commitLogger = context.actorOf(CommitLogCoordinator.props(), "commit-logger-coordinator")
      context.actorOf(
        WriteCoordinator
          .props(Some(commitLogger), metadataCoordinator, metricsSchemaActor, publisherActor)
          .withDispatcher("akka.actor.control-aware-dispatcher")
          .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
        s"write-coordinator_$nodeName"
      )
    } else
      context.actorOf(
        WriteCoordinator
          .props(None, metadataCoordinator, metricsSchemaActor, publisherActor)
          .withDeploy(Deploy(scope = RemoteScope(selfMember.address))),
        s"write-coordinator_$nodeName"
      )

  def receive: Receive = {
    case GetCoordinators => sender ! CoordinatorsGot(metadataCoordinator, writeCoordinator, readCoordinator)
    case GetPublisher    => sender() ! publisherActor
  }

}

object NodeActorsGuardian {
  def props(metadataCache: ActorRef) = Props(new NodeActorsGuardian(metadataCache))
}
