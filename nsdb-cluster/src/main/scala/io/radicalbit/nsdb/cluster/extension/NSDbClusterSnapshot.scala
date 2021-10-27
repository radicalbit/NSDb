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
import io.radicalbit.nsdb.common.protocol.NSDbNode

import java.util.UUID

/**
  * Extension that is inspired by the akka [[akka.cluster.Cluster]] extension with the purpose to store the current snapshot for a NSDb cluster.
  * Besides the (already provided by akka) [[akka.cluster.Member]] information, the unique node identifier is snapshot
  * and associated to an address, which may vary.
  */
class NSDbClusterSnapshotExtension(system: ExtendedActorSystem)
    extends Extension
    with SynchronizedMap[String, NSDbNode] {

  val instanceId = UUID.randomUUID()

  /**
    * Adds a node and associate it to the a unique identifier
    * @param address the actual address of the node.
    * @param nodeId the node unique identifier.
    * @param volatileId the node volatile id
    */
  def addNode(address: String, nodeId: String, volatileId: String): NSDbNode = {
    system.log.debug(s"adding node with address $address and $nodeId to ${values}")
//    threadSafeMap.put(address, NSDbNode(address, nodeId, volatileId))
    val node = NSDbNode(address, nodeId, volatileId)
    accumulate(address, node)
    node
  }

  def addNode(node: NSDbNode): NSDbNode = {
//    system.log.debug(s"adding node with address ${node.nodeAddress} to $threadSafeMap")
    accumulate(node.nodeAddress, node)
    node
  }

  /**
    * Removes a node.
    * @param address the actual node address.
    */
  def removeNode(address: String): Unit = {
//    system.log.error(s"removing node with address $address from ${values.map(e => e.nodeFsId)}")
//    system.log.error(s"removing from instanceId $instanceId")
//    val r = threadSafeMap.remove(address)
//    system.log.error(s"after removing address $address: ${threadSafeMap.asScala.map(e => (e._1, e._2.nodeFsId))}")
//    r

    pop(address)
    system.log.error(s"removed node with address $address from ${values.map(e => e.nodeFsId)}")
  }

  /**
    * Returns the current active nodes
    */
  def nodes: Iterable[NSDbNode] = {
//    system.log.error(s"getting from instanceId $instanceId")
//    threadSafeMap.asScala.values
    values
  }

  def getNode(address: String): NSDbNode = get(address).get //threadSafeMap.get(address)

}

object NSDbClusterSnapshot extends ExtensionId[NSDbClusterSnapshotExtension] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = NSDbClusterSnapshot

  override def createExtension(system: ExtendedActorSystem): NSDbClusterSnapshotExtension =
    new NSDbClusterSnapshotExtension(system)
}
