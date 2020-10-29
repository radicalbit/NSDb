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

package io.radicalbit.nsdb.cluster.metrics

import java.nio.file.{Files, Paths}

import akka.actor.Address
import akka.cluster.metrics.StandardMetrics._
import akka.cluster.metrics.{Metric, NodeMetrics}
import io.radicalbit.nsdb.cluster.metrics.NSDbMetrics._
import io.radicalbit.nsdb.test.NSDbSpec
import org.scalatest.OptionValues._

class DiskMetricsSelectorSpec extends NSDbSpec {

  val emptyNode      = Address("nsdb", "NSDb", "emptyNode", 2552)
  val almostFullNode = Address("nsdb", "NSDb", "node1", 2552)
  val node2          = Address("nsdb", "NSDb", "node2", 2552)
  val node3          = Address("nsdb", "NSDb", "node3", 2552)
  val node4          = Address("nsdb", "NSDb", "node4", 2552)
  val realNode       = Address("nsdb", "NSDb", "real", 2552)

  val fs = Files.getFileStore(Paths.get("."))

  val nodeMetrics1 = NodeMetrics(
    almostFullNode,
    System.currentTimeMillis,
    Set(
      Metric.create(DiskTotalSpace, 1000000, None),
      Metric.create(DiskFreeSpace, 100, None),
      Metric.create(HeapMemoryMax, 512, None),
      Metric.create(CpuCombined, 0.2, None),
      Metric.create(CpuStolen, 0.1, None),
      Metric.create(SystemLoadAverage, 0.5, None),
      Metric.create(Processors, 8, None)
    ).flatten
  )

  val emptyNodeMetric = NodeMetrics(
    emptyNode,
    System.currentTimeMillis,
    Set(Metric.create(DiskTotalSpace, 1000000, None), Metric.create(DiskFreeSpace, 0, None)).flatten
  )

  val nodeMetrics2 = NodeMetrics(
    node2,
    System.currentTimeMillis,
    Set(Metric.create(DiskTotalSpace, 1000000, None), Metric.create(DiskFreeSpace, 750000, None)).flatten
  )

  val nodeMetrics3 = NodeMetrics(
    node3,
    System.currentTimeMillis,
    Set(Metric.create(DiskTotalSpace, 1000000, None), Metric.create(DiskFreeSpace, 1000000, None)).flatten
  )

  val nodeMetrics4 = NodeMetrics(
    node4,
    System.currentTimeMillis,
    Set()
  )

  val realNodeMetrics = NodeMetrics(
    realNode,
    System.currentTimeMillis,
    Set(Metric.create(DiskTotalSpace, fs.getTotalSpace, None), Metric.create(DiskFreeSpace, fs.getUsableSpace, None)).flatten
  )

  val nodeMetrics = Set(emptyNodeMetric, nodeMetrics1, nodeMetrics2, nodeMetrics3, nodeMetrics4, realNodeMetrics)

  "DiskMetricsSelector" must {
    "calculate capacity of heap metrics" in {
      val capacity = DiskMetricsSelector.capacity(nodeMetrics)
      capacity.get(emptyNode) shouldBe Some(0.0)
      capacity.get(almostFullNode) shouldBe Some(0.0001)
      capacity.get(node2) shouldBe Some(0.75)
      capacity.get(node3) shouldBe Some(1)
      capacity.get(node4) shouldBe None
      //for a real node the capacity must be between 0 and 1. There's no way to estimate a reasonable capacity value and mocking is not the point here
      capacity.get(realNode).value shouldBe >(0.0)
      capacity.get(realNode).value shouldBe <(1.0)
    }
  }

}
