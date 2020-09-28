/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import akka.actor.ActorRef
import akka.cluster.Member
import io.radicalbit.nsdb.cluster.NsdbNodeEndpoint

final class ClusterListener extends AbstractClusterListener {
  override def enableClusterMetricsExtension: Boolean = true

  protected def onSuccessBehaviour(readCoordinator: ActorRef,
                                   writeCoordinator: ActorRef,
                                   metadataCoordinator: ActorRef,
                                   publisherActor: ActorRef): Unit =
    new NsdbNodeEndpoint(nodeId, readCoordinator, writeCoordinator, metadataCoordinator, publisherActor)(context.system)

  protected def onFailureBehaviour(member: Member, error: Any): Unit = {
    log.error("received wrong response {}", error)
    cluster.leave(member.address)
  }
}
