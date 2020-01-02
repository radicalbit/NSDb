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

import akka.cluster.metrics._
import akka.actor.Address
import io.radicalbit.nsdb.cluster.metrics.{DiskMetricsSelector, NSDbMixMetricSelector}
import org.scalatest.{Matchers, WordSpec}

/**
  * only the disk metric selector is tested because the other selectors have been already accurately tested inside the akka project.
  */
class CapacityWriteNodesSelectionLogicSpec extends WordSpec with Matchers {

  def akkaClusterMetric: Set[Metric] =
    Set(
      Metric("heap-memory-used", 68038160, Some(EWMA(6.774968476043373E7, 0.438770294474789))),
      Metric("heap-memory-max", 7635730432L, None),
      Metric("cpu-combined", 0.2065661194900425, Some(EWMA(0.1316384958460451, 0.438770294474789))),
      Metric("processors", 12, None),
      Metric("system-load-average", 4.23193359375, None),
      Metric("heap-memory-committed", 660602880, Some(EWMA(6.6060288E8, 0.438770294474789))),
      Metric("cpu-stolen", 0.0, Some(EWMA(0.0, 0.438770294474789)))
    )

  val nodeMetricsEqualDiskCapacity = Set(
    NodeMetrics(
      Address("akka", "NSDb", "127.0.0.1", 2552),
      System.currentTimeMillis(),
      akkaClusterMetric ++
        Set(Metric("disk_free_space", 344051822592L, None), Metric("disk_total_space", 500000000000L, None))
    ),
    NodeMetrics(
      Address("akka", "NSDb", "127.0.0.3", 2552),
      System.currentTimeMillis(),
      akkaClusterMetric ++
        Set(Metric("disk_free_space", 344051822592L, None), Metric("disk_total_space", 499963174912L, None))
    ),
    NodeMetrics(
      Address("akka", "NSDb", "127.0.0.2", 2552),
      System.currentTimeMillis(),
      akkaClusterMetric ++
        Set(Metric("disk_free_space", 344051822592L, None), Metric("disk_total_space", 499963174912L, None))
    )
  )

  val fullDiskNode = NodeMetrics(
    Address("akka", "NSDb", "127.0.0.2", 2552),
    System.currentTimeMillis(),
    akkaClusterMetric ++
      Set(Metric("disk_free_space", 0, None), Metric("disk_total_space", 500000000000L, None))
  )

  val emptyDiskNode = NodeMetrics(
    Address("akka", "NSDb", "127.0.0.2", 2552),
    System.currentTimeMillis(),
    akkaClusterMetric ++
      Set(Metric("disk_free_space", 500000000000L, None), Metric("disk_total_space", 500000000000L, None))
  )

  val ordinaryNodeMetrics = Set(
    NodeMetrics(
      Address("akka", "NSDb", "127.0.0.1", 2552),
      System.currentTimeMillis(),
      akkaClusterMetric ++
        Set(Metric("disk_free_space", 350000000000L, None), Metric("disk_total_space", 500000000000L, None))
    ),
    NodeMetrics(
      Address("akka", "NSDb", "127.0.0.3", 2552),
      System.currentTimeMillis(),
      akkaClusterMetric ++
        Set(Metric("disk_free_space", 200000000000L, None), Metric("disk_total_space", 500000000000L, None))
    )
  )

  "CapacityWriteNodesSelectionLogic" should {
    "select metric selector from config" in {
      CapacityWriteNodesSelectionLogic.fromConfigValue("heap") shouldBe HeapMetricsSelector
      CapacityWriteNodesSelectionLogic.fromConfigValue("load") shouldBe SystemLoadAverageMetricsSelector
      CapacityWriteNodesSelectionLogic.fromConfigValue("cpu") shouldBe CpuMetricsSelector
      CapacityWriteNodesSelectionLogic.fromConfigValue("disk") shouldBe DiskMetricsSelector
      CapacityWriteNodesSelectionLogic.fromConfigValue("mix") shouldBe NSDbMixMetricSelector
      an[IllegalArgumentException] should be thrownBy CapacityWriteNodesSelectionLogic.fromConfigValue("wrong")
    }
  }

  "CapacityWriteNodesSelectionLogic" when {
    "cluster nodes have the same disk capacity" should {
      "select write nodes based on DiskMetricsSelector" in {
        //just test that the method return the correct amount of elements
        new CapacityWriteNodesSelectionLogic(DiskMetricsSelector)
          .selectWriteNodes(nodeMetricsEqualDiskCapacity, 2)
          .size shouldBe 2
      }
    }

    "one cluster node has got 0 capacity" should {
      "select nodes with the highest disk" in {
        new CapacityWriteNodesSelectionLogic(DiskMetricsSelector)
          .selectWriteNodes(ordinaryNodeMetrics + fullDiskNode, 2) shouldBe Seq("127.0.0.1_2552", "127.0.0.3_2552")
      }
    }

    "one cluster node has got full capacity" should {
      "select nodes with the highest disk" in {
        new CapacityWriteNodesSelectionLogic(DiskMetricsSelector)
          .selectWriteNodes(ordinaryNodeMetrics + emptyDiskNode, 2) shouldBe Seq("127.0.0.2_2552", "127.0.0.1_2552")
      }
    }
  }
}
