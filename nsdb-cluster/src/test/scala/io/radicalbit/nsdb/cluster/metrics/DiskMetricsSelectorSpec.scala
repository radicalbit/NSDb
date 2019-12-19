package io.radicalbit.nsdb.cluster.metrics

import akka.actor.Address
import akka.cluster.metrics.StandardMetrics._
import akka.cluster.metrics.{Metric, NodeMetrics}
import io.radicalbit.nsdb.cluster.metrics.NSDbMetrics._
import org.scalatest.{Matchers, WordSpec}

class DiskMetricsSelectorSpec extends WordSpec with Matchers {

  val node1 = Address("nsdb", "NSDb", "node1", 2552)
  val node2 = Address("nsdb", "NSDb", "node2", 2552)
  val node3 = Address("nsdb", "NSDb", "node3", 2552)

  val nodeMetrics1 = NodeMetrics(
    node1,
    System.currentTimeMillis,
    Set(
      Metric.create(DiskTotalSpace, 1000000, None),
      Metric.create(DiskFreeSpace, 100, None),
      Metric.create(HeapMemoryMax, 512, None),
      Metric.create(CpuCombined, 0.2, None),
      Metric.create(CpuStolen, 0.1, None),
      Metric.create(SystemLoadAverage, 0.5, None),
      Metric.create(Processors, 8, None)).flatten
  )

  val nodeMetrics2 = NodeMetrics(
    node2,
    System.currentTimeMillis,
    Set(
      Metric.create(DiskTotalSpace, 1000000, None),
      Metric.create(DiskFreeSpace, 750000, None)).flatten
    )

  val nodeMetrics3 = NodeMetrics(
    node3,
    System.currentTimeMillis,
    Set()
  )

  val nodeMetrics = Set(nodeMetrics1, nodeMetrics2, nodeMetrics3)

  "DiskMetricsSelector" must {
    "calculate capacity of heap metrics" in {
      val capacity = DiskMetricsSelector.capacity(nodeMetrics)
      capacity.get(node1) shouldBe Some(0.9999)
      capacity.get(node2) shouldBe Some(0.25)
      capacity.get(node3) shouldBe None
    }
  }

}
