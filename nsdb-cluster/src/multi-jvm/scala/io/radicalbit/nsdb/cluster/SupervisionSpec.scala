package io.radicalbit.nsdb.cluster

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.MemberStatus
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.NodeActorGuardianFixedNamesForTest
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.common.protocol.NSDbNode
import io.radicalbit.nsdb.common.statement.{AllFields, SelectSQLStatement}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementExecuted
import io.radicalbit.nsdb.{NSDbMultiNodeFixedNamesActorsSupport, NSDbMultiNodeSpec}

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class SupervisionSpecMultiJvmNode1 extends SupervisionSpec
class SupervisionSpecMultiJvmNode2 extends SupervisionSpec
class SupervisionSpecMultiJvmNode3 extends SupervisionSpec

object SupervisionSpecConfig extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")

  commonConfig(ConfigFactory.parseResources("application.conf"))

  nodeConfig(node1)(ConfigFactory.parseString(
    """
       |akka.remote.artery.canonical.port = 25520
    """.stripMargin))

  nodeConfig(node2)(ConfigFactory.parseString(
    """
      |akka.remote.artery.canonical.port = 25530
    """.stripMargin))

  nodeConfig(node3)(ConfigFactory.parseString(
    """
      |akka.remote.artery.canonical.port = 25540
    """.stripMargin))


}

class NodeActorGuardianForTestWithFailingCoordinator(nodeFsId: String, volatileId : String, probe: ActorRef) extends NodeActorGuardianFixedNamesForTest(Some(nodeFsId), volatileId) {

  class FailingReadCoordinator(probe: ActorRef) extends Actor {

    val internalState = UUID.randomUUID().toString

    node = NSDbNode(nodeAddress, nodeFsId, volatileId)

    override def preStart(): Unit =
      log.error(s"failing read coordinator started at path ${self.path}")

    override def receive: Receive = {
      case "GetInternalState" => sender() ! internalState
      case ExecuteStatement(statement, _) if statement.db == "failingDb" =>
        probe ! "Failure"
        throw new RuntimeException("it's failing")
      case ExecuteStatement(statement, _)  =>
        sender ! SelectStatementExecuted(statement, Seq.empty, null)
    }
  }

  override protected lazy val readCoordinator: ActorRef =
    context.actorOf(Props( new FailingReadCoordinator(probe)), s"read-coordinator_$actorNameSuffix")

}

object NodeActorGuardianForTestWithFailingCoordinator {
  def props(nodeFsId: String, volatileId: String, probe: ActorRef): Props = Props(new NodeActorGuardianForTestWithFailingCoordinator(nodeFsId, volatileId, probe))
}

class SupervisionSpec extends MultiNodeSpec(SupervisionSpecConfig) with NSDbMultiNodeSpec with NSDbMultiNodeFixedNamesActorsSupport with ImplicitSender {

  import SupervisionSpecConfig._

  def initialParticipants: Int = roles.size

  val probe = TestProbe()

  val correctStatement = ExecuteStatement(SelectSQLStatement(
    db = "db",
    namespace = "namespace",
    metric = "metric",
    distinct = false,
    fields = AllFields(),
    None
  ))

  val errorStatement = ExecuteStatement(SelectSQLStatement(
    db = "failingDb",
    namespace = "namespace",
    metric = "metric",
    distinct = false,
    fields = AllFields(),
    None
  ))

  override lazy val nodeActorGuardianProp = Props(new NodeActorGuardianForTestWithFailingCoordinator(selfNodeFsId, volatileId, probe.ref))

  "ClusterListener" must {
    "successfully set up a cluster with a potentially failing node" in {

      cluster.join(node(node1).address)
      cluster.join(node(node1).address)
      cluster.join(node(node1).address)

      awaitAssert {
        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe 3
        NSDbClusterSnapshot(system).nodes.size shouldBe 3
      }
      enterBarrier(5 seconds, "nodes joined")
    }


    "handle errors and properly resume child actor" in {
      readCoordinator ! correctStatement

      expectMsgType[SelectStatementExecuted]

      readCoordinator ! "GetInternalState"

      val internalState = expectMsgType[String]

      readCoordinator ! errorStatement

      probe.expectMsg("Failure")
      probe.expectNoMessage(5 seconds)


      expectNoMessage(5 seconds)

      readCoordinator ! correctStatement

      expectMsgType[SelectStatementExecuted]

      readCoordinator ! "GetInternalState"

      expectMsgType[String] shouldBe internalState
    }

    "handle errors and terminate child actor when resumes exceeds the limits" in {
        readCoordinator.resolveOne(1 seconds).onComplete {
          case Success(readCoordinatorActor) =>
            readCoordinatorActor ! errorStatement
            readCoordinatorActor ! errorStatement
            readCoordinatorActor ! errorStatement

            probe.expectMsg("Failure")
            probe.expectMsg("Failure")
            probe.expectMsg("Failure")

            val terminationPrrobe = TestProbe()
            terminationPrrobe.watch(readCoordinatorActor)
            terminationPrrobe.expectTerminated(readCoordinatorActor)
          case Failure(ex) => fail(ex)
        }
    }

  }

}
