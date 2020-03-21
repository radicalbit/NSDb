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

package io.radicalbit.nsdb.cluster.extension

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import scala.collection.JavaConverters._

class NSDbClusterSnapshotExtension(system: ExtendedActorSystem) extends Extension {

  import java.util
  import java.util.Collections
  import java.util.concurrent.ConcurrentHashMap

  private val threadSafeSet: util.Set[String] =
    Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean])

  def addNodeName(nodeName: String): Boolean    = threadSafeSet.add(nodeName)
  def removeNodeName(nodeName: String): Boolean = threadSafeSet.remove(nodeName)

  def nodes: Set[String] = threadSafeSet.asScala.toSet
}

object NSDbClusterSnapshot extends ExtensionId[NSDbClusterSnapshotExtension] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = NSDbClusterSnapshot

  override def createExtension(system: ExtendedActorSystem): NSDbClusterSnapshotExtension =
    new NSDbClusterSnapshotExtension(system)
}
