package io.radicalbit.nsdb

import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.MemberStatus.Up
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.ImplicitSender
import io.radicalbit.rtsae.STMultiNodeSpec

abstract class SplitBrainSpec(config: MultiNodeConfig)
  extends MultiNodeSpec(config)
    with STMultiNodeSpec
    with ImplicitSender {

  def initialParticipants: Int = roles.size

  protected lazy val cluster: Cluster = Cluster(system)

  private val addresses: Map[RoleName, Address] = roles.map(r => r -> node(r).address).toMap

  protected def addressOf(roleName: RoleName): Address = addresses(roleName)

  protected def waitForUp(roleNames: RoleName*): Unit = awaitCond {
    roleNames.forall(
      role => cluster.state.members.exists(m => m.address === addressOf(role) && m.status === Up)
    )
  }

  protected def assertLeader(nodesInCluster: RoleName*): Unit =
    if (nodesInCluster.contains(myself)) {
      nodesInCluster.length should not be 0
      awaitCond(nodesInCluster.map(addressOf).contains(cluster.state.getLeader))
    }

  protected def assertUnreachable(unreachableNodes: RoleName*): Unit =
    awaitCond(cluster.state.unreachable.map(_.address) == unreachableNodes.map(addressOf).toSet)

  protected def switchOffConnection(from: RoleName, to: RoleName) =
    testConductor.blackhole(from, to, Direction.Both).await

}
