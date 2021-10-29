package io.radicalbit.nsdb

import akka.actor.{ActorSelection, Props}
import akka.cluster.Member
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.VOLATILE_ID_LENGTH
import io.radicalbit.nsdb.cluster.actor.NodeActorsGuardian.{GetNode, NodeGot}
import io.radicalbit.nsdb.cluster.actor.{NodeActorGuardianFixedNamesForTest, NodeActorGuardianForTestDynamicNames}
import io.radicalbit.nsdb.common.protocol.NSDbNode
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.OptionValues

trait NSDbMultiNodeActorsSupport { self: NSDbMultiNodeSpec =>

  lazy val selfMember: Member = cluster.selfMember
  lazy val nodeAddress   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
  lazy val selfNodeFsId: String = myself.name

  def nodeActorGuardianProp: Props

  protected def actorFromPath(pathFunction: String): ActorSelection = {
    cluster.system.actorSelection(pathFunction)
  }

  lazy val nodeActorGuardian: ActorSelection = actorFromPath(s"user/guardian_${nodeAddress}_$selfNodeFsId")

  override def beforeAll(): Unit = {
    multiNodeSpecBeforeAll()
    system.actorOf(nodeActorGuardianProp, name = s"guardian_${nodeAddress}_$selfNodeFsId")
  }
}

trait NSDbMultiNodeFixedNamesActorsSupport extends NSDbMultiNodeActorsSupport  { self: NSDbMultiNodeSpec =>
  lazy val volatileId = RandomStringUtils.randomAlphabetic(VOLATILE_ID_LENGTH)
  lazy val nodeActorGuardianProp: Props = NodeActorGuardianFixedNamesForTest.props(selfNodeFsId, volatileId)

  val metadataCoordinatorPath = s"user/guardian_${nodeAddress}_$selfNodeFsId/metadata-coordinator_${nodeAddress}_${selfNodeFsId}_$volatileId"
  val schemaCoordinatorPath = s"user/guardian_${nodeAddress}_$selfNodeFsId/schema-coordinator_${nodeAddress}_${selfNodeFsId}_$volatileId"
  val writeCoordinatorPath = s"user/guardian_${nodeAddress}_$selfNodeFsId/write-coordinator_${nodeAddress}_${selfNodeFsId}_$volatileId"
  val readCoordinatorPath = s"user/guardian_${nodeAddress}_$selfNodeFsId/read-coordinator_${nodeAddress}_${selfNodeFsId}_$volatileId"
  val metadataCachePath = s"user/guardian_${nodeAddress}_$selfNodeFsId/metadata-cache_${nodeAddress}_${selfNodeFsId}_$volatileId"

  lazy val metadataCoordinator: ActorSelection = actorFromPath(metadataCoordinatorPath)
  lazy val schemaCoordinator: ActorSelection = actorFromPath(schemaCoordinatorPath)
  lazy val readCoordinator: ActorSelection = actorFromPath(readCoordinatorPath)
  lazy val metadataCache: ActorSelection = actorFromPath(metadataCachePath)

}

trait NSDbMultiNodeDynamicNameActorsSupport extends NSDbMultiNodeActorsSupport with OptionValues { self: NSDbMultiNodeSpec =>

  import akka.pattern.ask

  lazy val nodeActorGuardianProp: Props = NodeActorGuardianForTestDynamicNames.props(selfNodeFsId)

  implicit def timeout: Timeout

  def metadataCoordinatorPath(volatileId: String) = s"user/guardian_${nodeAddress}_$selfNodeFsId/metadata-coordinator_${nodeAddress}_${selfNodeFsId}_$volatileId"
  def schemaCoordinatorPath(volatileId: String) = s"user/guardian_${nodeAddress}_$selfNodeFsId/schema-coordinator_${nodeAddress}_${selfNodeFsId}_$volatileId"
  def writeCoordinatorPath(volatileId: String) = s"user/guardian_${nodeAddress}_$selfNodeFsId/write-coordinator_${nodeAddress}_${selfNodeFsId}_$volatileId"
  def readCoordinatorPath(volatileId: String) = s"user/guardian_${nodeAddress}_$selfNodeFsId/read-coordinator_${nodeAddress}_${selfNodeFsId}_$volatileId"
  def metadataCachePath(volatileId: String) = s"user/guardian_${nodeAddress}_$selfNodeFsId/metadata-cache_${nodeAddress}_${selfNodeFsId}_$volatileId"

  def metadataCoordinator(volatileId: String = selfNode.volatileNodeUuid): ActorSelection = actorFromPath(metadataCoordinatorPath(volatileId))
  def schemaCoordinator(volatileId: String= selfNode.volatileNodeUuid): ActorSelection = actorFromPath(schemaCoordinatorPath(volatileId))
  def readCoordinator(volatileId: String= selfNode.volatileNodeUuid): ActorSelection = actorFromPath(readCoordinatorPath(volatileId))
  def metadataCache(volatileId: String= selfNode.volatileNodeUuid): ActorSelection = actorFromPath(metadataCachePath(volatileId))

  def selfNode: NSDbNode = (nodeActorGuardian ? GetNode).mapTo[NodeGot].await.node.value
}
