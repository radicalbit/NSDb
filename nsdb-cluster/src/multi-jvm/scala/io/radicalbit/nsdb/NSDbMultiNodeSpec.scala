package io.radicalbit.nsdb

import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeSpec
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.ImplicitSender
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest.BeforeAndAfterAll

/**
  * Hooks up MultiNodeSpec with ScalaTest
  */
trait NSDbMultiNodeSpec extends MultiNodeSpec with NSDbSpecLike with BeforeAndAfterAll with ImplicitSender{

  protected implicit lazy val cluster: Cluster = Cluster(system)

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {

      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()
}

trait NSDbMultiNodeFailuresSupport {self: MultiNodeSpec =>

  protected def switchOffConnection(from: RoleName, to: RoleName): Unit =
    testConductor.blackhole(from, to, Direction.Send).await

  protected def switchOnConnection(from: RoleName, to: RoleName): Unit =
    testConductor.passThrough(from, to, Direction.Both).await

  protected def exit(role: RoleName, exitCode: Int = 0): Unit =
    testConductor.exit(role, exitCode).await
}