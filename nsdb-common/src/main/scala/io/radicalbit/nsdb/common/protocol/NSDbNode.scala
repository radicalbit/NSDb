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

package io.radicalbit.nsdb.common.protocol

/**
  * A NSDb node must contain an identifier associated to the akka cluster address
  * and an identifier associated to the file system, i.e. the concrete volume in which shards are stored.
  * @param nodeAddress the akka cluster node address.
  * @param nodeFsId the file system identifier.
  * @param volatileNodeUuid needed to be able to distinguish different incarnations of a node with same nodeAddress and nodeFsId.
  */
case class NSDbNode(nodeAddress: String, nodeFsId: String, volatileNodeUuid: String) extends NSDbSerializable {
  def uniqueNodeId: String = s"${nodeAddress}_${nodeFsId}_$volatileNodeUuid"
}

object NSDbNode {

  def fromUniqueId(uniqueIdentifier: String): NSDbNode = {
    val components = uniqueIdentifier.split('_')
    NSDbNode(components(0), components(1), components(2))
  }

  def empty: NSDbNode = NSDbNode("", "", "")
}
