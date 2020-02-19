package io.radicalbit.nsdb.split_brain

import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.MemberStatus.{Down, Exiting, Removed, Up}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.ImplicitSender
import io.radicalbit.rtsae.STMultiNodeSpec

import scala.concurrent.duration.Duration

/**
  * Base class for split brain test
  * The functions names have been inspired by the lithium plugin multi jvm tests
  * @param config multi node configuration
  */
abstract class MultiNodeBaseSpec(config: MultiNodeConfig)
    extends MultiNodeSpec(config)
    with STMultiNodeSpec
    with ImplicitSender {

  def initialParticipants: Int = roles.size

  protected lazy val cluster: Cluster = Cluster(system)

  private val addresses: Map[RoleName, Address] = roles.map(r => r -> node(r).address).toMap

  protected def addressOf(roleName: RoleName): Address = addresses(roleName)

  protected def awaitClusterNodesForUp(roleNames: RoleName*): Unit = awaitCond {
    roleNames.forall(
      role => cluster.state.members.exists(m => m.address === addressOf(role) && m.status === Up)
    )
  }

  protected def awaitClusterLeader(nodesInCluster: RoleName*): Unit =
    if (nodesInCluster.contains(myself)) {
      nodesInCluster.length should not be 0
      awaitCond(nodesInCluster.map(addressOf).contains(cluster.state.getLeader))
    }

  protected def awaitUnreachableNodes(unreachableNodes: RoleName*): Unit =
    awaitCond(cluster.state.unreachable.map(_.address) === unreachableNodes.map(addressOf).toSet)

  protected def switchOffConnection(from: RoleName, to: RoleName) =
    testConductor.blackhole(from, to, Direction.Both).await

  protected def awaitSurvivorsNodes(roleNames: RoleName*): Unit =
    awaitCond(roleNames.forall(role => cluster.state.members.exists(_.address === addressOf(role))))

  protected def awaitAllLeavingNodes(roleNames: RoleName*): Unit =
    awaitCond(roleNames.forall { role =>
      val members     = cluster.state.members
      val unreachable = cluster.state.unreachable

      val address = addressOf(role)

      unreachable.isEmpty &&
      (members.exists(m => m.address === address && (m.status === Down || m.status === Exiting)) ||
      !members.exists(_.address === address))
    })

  protected def awaitSelfDowningNode(max: Duration = Duration.Undefined) =
    awaitCond(
      {
        val selfAddress = cluster.selfAddress
        cluster.state.members.exists(m =>
          m.address === selfAddress && (m.status === Exiting || m.status === Down || m.status === Removed))
      },
      max
    )

  protected def awaitExistingMembers(roleNames: RoleName*): Unit =
    awaitCond(cluster.state.members.map(_.address) === roleNames.map(addressOf).toSet)

}
