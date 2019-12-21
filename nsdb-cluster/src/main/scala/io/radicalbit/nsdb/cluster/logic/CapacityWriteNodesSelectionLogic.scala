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

package io.radicalbit.nsdb.cluster.logic

import akka.cluster.metrics.{CapacityMetricsSelector, NodeMetrics}
import io.radicalbit.nsdb.cluster._

/**
  * Node selection logic based on nodes capacity, that is retrieved from the node metrics and a selector
  * @param metricsSelector selects a certain metric (e.g. disk, heap) and calculates the nodes capacity based on it.
  */
class CapacityWriteNodesSelectionLogic(metricsSelector: CapacityMetricsSelector) extends WriteNodesSelectionLogic {

  override def selectWriteNodes(nodeMetrics: Set[NodeMetrics], replicationFactor: Int): Seq[String] = {
    metricsSelector.capacity(nodeMetrics).toList.sortBy { case (_, m) => m }.reverse.take(replicationFactor).map {
      case (address, _) => createNodeName(address)
    }
  }
}
