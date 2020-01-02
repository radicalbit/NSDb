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

import akka.cluster.metrics.{
  CapacityMetricsSelector,
  CpuMetricsSelector,
  HeapMetricsSelector,
  NodeMetrics,
  SystemLoadAverageMetricsSelector
}
import io.radicalbit.nsdb.cluster._
import io.radicalbit.nsdb.cluster.metrics.{DiskMetricsSelector, NSDbMixMetricSelector}

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

object CapacityWriteNodesSelectionLogic {

  /**
    * Provides a metric selector from a config value.
    * @param configValue the config value provided in the NSDb configuration file
    * @return the specified metric selector
    * @throws IllegalArgumentException the configuration is not among the allowed values.
    */
  def fromConfigValue(configValue: String): CapacityMetricsSelector = {
    configValue match {
      case "heap" => HeapMetricsSelector
      case "load" => SystemLoadAverageMetricsSelector
      case "cpu"  => CpuMetricsSelector
      case "disk" => DiskMetricsSelector
      case "mix"  => NSDbMixMetricSelector
      case wrongConfigValue =>
        throw new IllegalArgumentException(s"$wrongConfigValue is not a valid value for metric-selector")
    }
  }
}
