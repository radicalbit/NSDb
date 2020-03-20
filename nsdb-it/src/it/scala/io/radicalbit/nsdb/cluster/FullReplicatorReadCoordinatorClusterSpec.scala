package io.radicalbit.nsdb.cluster

/**
 * Test that ensure Metric Reader Actor correctness logic
 */
class FullReplicatorReadCoordinatorClusterSpec extends ReadCoordinatorClusterSpec {
  override val replicationFactor: Int = 3
}
