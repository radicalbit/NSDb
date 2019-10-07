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
import akka.actor._
import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian.{GetMetadataCache, GetSchemaCache}

/**
  * Actor that creates all the global actors (e.g. metadata cache, cluster listener)
  */
class DatabaseActorsGuardian extends Actor with ActorLogging {

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: TimeoutException =>
      log.error(e, "Got the following TimeoutException, resuming the processing")
      Resume
    case t =>
      log.error(t, "generic error occurred")
      super.supervisorStrategy.decider.apply(t)
  }

  def receive: Receive = {
    case GetMetadataCache(nodeName) =>
      sender ! context.system.actorOf(Props[ReplicatedMetadataCache], s"metadata-cache-$nodeName")
    case GetSchemaCache(nodeName) =>
      sender ! context.system.actorOf(Props[ReplicatedSchemaCache], s"schema-cache-$nodeName")
  }
}

object DatabaseActorsGuardian {
  case class GetMetadataCache(nodeName: String)
  case class GetSchemaCache(nodeName: String)
}
