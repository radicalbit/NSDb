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

package io.radicalbit.nsdb
import akka.actor.Address
import akka.cluster.Member

package object cluster {

  def createNodeName(member: Member) =
    s"${member.address.host.getOrElse("noHost")}_${member.address.port.getOrElse(2552)}"

  def createNodeName(address: Address) =
    s"${address.host.getOrElse("noHost")}_${address.port.getOrElse(2552)}"

  /**
    * Creates a fake address with a dedicated (and invented) `nsdb` protocol
    * @param nodeName the node name [host]_[port]
    */
  def createAddress(nodeName: String): Address = {
    val splittedNodeName = nodeName.split("_")
    Address("nsdb",
            "NSDb",
            Option(splittedNodeName(0)).getOrElse("noHost"),
            Option(splittedNodeName(1)).map(_.toInt).getOrElse(2552))
  }

  final object PubSubTopics {
    final val COORDINATORS_TOPIC   = "coordinators"
    final val NODE_GUARDIANS_TOPIC = "node-guardians"
    final val NSDB_METRICS_TOPIC   = "nsdb-metrics"
  }
}
