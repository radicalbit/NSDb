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

package io.radicalbit.nsdb.cluster.actor

import akka.cluster.metrics.NodeMetrics
import io.radicalbit.nsdb.common.protocol.NSDbSerializable

object NSDbMetricsEvents {

  /**
    * Event fired when akka cluster metrics are collected that described the disk occupation ration for a node
    * @param nodeName cluster node name.
    * @param usableSpace the free space on disk.
    * @param totalSpace total disk space.
    */
  case class DiskOccupationChanged(nodeName: String, usableSpace: Long, totalSpace: Long) extends NSDbSerializable

  case object GetNodeMetrics extends NSDbSerializable

  /**
    * Contains the metrics for each alive member of the cluster.
    * @param nodeMetrics one entry contains all the metrics for a single node.
    */
  case class NodeMetricsGot(nodeMetrics: Set[NodeMetrics]) extends NSDbSerializable

}
