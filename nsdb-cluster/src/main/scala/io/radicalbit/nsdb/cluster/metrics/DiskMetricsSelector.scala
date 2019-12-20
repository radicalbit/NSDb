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

package io.radicalbit.nsdb.cluster.metrics

import akka.actor.Address
import akka.cluster.metrics.{CapacityMetricsSelector, NodeMetrics}
import io.radicalbit.nsdb.cluster.metrics.NSDbMetrics.Disk

case object DiskMetricsSelector extends CapacityMetricsSelector {

  /**
    * Remaining capacity for each node. The value is between
    * 0.0 and 1.0, where 0.0 means no remaining capacity (full
    * utilization) and 1.0 means full remaining capacity (zero
    * utilization).
    */
  override def capacity(nodeMetrics: Set[NodeMetrics]): Map[Address, Double] = {
    nodeMetrics.collect {
      case Disk(address, freeSpace, totalSpace) =>
        val capacity = 1.0 - freeSpace.toDouble / totalSpace
        (address, capacity)
    }.toMap
  }
}
