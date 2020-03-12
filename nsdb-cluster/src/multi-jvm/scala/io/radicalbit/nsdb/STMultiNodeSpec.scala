package io.radicalbit.rtsae

import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.Matchers
import akka.remote.testkit.{MultiNodeSpec, MultiNodeSpecCallbacks}

/**
  * Hooks up MultiNodeSpec with ScalaTest
  */
trait STMultiNodeSpec extends MultiNodeSpecCallbacks with WordSpecLike with Matchers with BeforeAndAfterAll { this: MultiNodeSpec =>

  protected lazy val cluster: Cluster = Cluster(system)

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {

      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()
}
