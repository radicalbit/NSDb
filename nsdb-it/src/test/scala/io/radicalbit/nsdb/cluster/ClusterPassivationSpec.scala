package io.radicalbit.nsdb.cluster

class ClusterPassivationSpec extends ReadCoordinatorClusterSpec {

  override val passivateAfter = java.time.Duration.ofSeconds(10)

  override def beforeAll(): Unit = {
    super.beforeAll()
    waitPassivation()
    waitPassivation()
  }
}
