package io.radicalbit.nsdb

import akka.actor.ActorSelection
import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeSpec, MultiNodeSpecCallbacks}
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest.BeforeAndAfterAll

/**
  * Hooks up MultiNodeSpec with ScalaTest
  */
trait STMultiNodeSpec extends MultiNodeSpecCallbacks with NSDbSpecLike with BeforeAndAfterAll { this: MultiNodeSpec =>

  protected implicit lazy val cluster: Cluster = Cluster(system)

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {

      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  def getActorPath(pathFunction: String => String)(implicit cluster: Cluster): ActorSelection = {
    val selfMember = cluster.selfMember
    val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
    cluster.system.actorSelection(pathFunction(nodeName))
  }

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()
}
