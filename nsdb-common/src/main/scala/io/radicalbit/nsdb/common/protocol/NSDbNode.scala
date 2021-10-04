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

case class NSDbNode(nodeAddress: String, nodeFsId: String) extends NSDbSerializable {
  def uniqueNodeId: String = s"${nodeAddress}_$nodeFsId"
}

object NSDbNode {

  def fromUniqueId(uniqueIdentifier: String): NSDbNode = {
    val components = uniqueIdentifier.split('_')
    NSDbNode(components(0), components(1))
  }

  def empty: NSDbNode = NSDbNode("", "")
}
