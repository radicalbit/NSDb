package io.radicalbit.nsdb

import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeSpec, MultiNodeSpecCallbacks}
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest.BeforeAndAfterAll

/**
  * Hooks up MultiNodeSpec with ScalaTest
  */
trait STMultiNodeSpec extends MultiNodeSpecCallbacks with NSDbSpecLike with BeforeAndAfterAll { this: MultiNodeSpec =>

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
