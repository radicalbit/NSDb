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

package io.radicalbit.nsdb.minicluster

import akka.actor.{ActorRef, Props}
import com.typesafe.scalalogging.LazyLogging
import io.radicalbit.nsdb.cluster.{NSDBAActors, NSDBAkkaCluster}
import io.radicalbit.nsdb.common.NsdbConfig

/**
  * Overrides node actor props in order to inject custom behaviours useful for test purpose.
  */
trait TestCluster extends NSDBAkkaCluster with NSDBAActors with NsdbConfig {
  override def nodeActorGuardianProps(metadataCache: ActorRef, schemaCache: ActorRef): Props =
    super.nodeActorGuardianProps(metadataCache, schemaCache)
}

trait NSDbMiniClusterDefinition extends TestCluster with NsdbMiniClusterConf with LazyLogging {}
