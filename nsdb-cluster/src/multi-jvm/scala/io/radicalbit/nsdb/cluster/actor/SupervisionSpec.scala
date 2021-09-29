package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.{Member, MemberStatus}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.STMultiNodeSpec
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.common.statement.{AllFields, SelectSQLStatement}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementExecuted

import java.util.UUID
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

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

class NodeActorGuardianForTestWithFailingCoordinator(probe: ActorRef) extends NodeActorGuardianForTest {

  class FailingReadCoordinator(probe: ActorRef) extends Actor {

    val volatileDb = UUID.randomUUID().toString

    override def preStart(): Unit =
      log.error(s"failing read coordinator started at path ${self.path}")

    override def receive: Receive = {
      case "GetVolatileId" => sender() ! volatileDb
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

class SupervisionSpec extends MultiNodeSpec(SupervisionSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import SupervisionSpecConfig._

  def initialParticipants: Int = roles.size

  val selfMember: Member = cluster.selfMember
  val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

  val probe = TestProbe()

  private def readCoordinatorPath(nodeName: String) = s"user/guardian_${nodeName}_$nodeName/read-coordinator_${nodeName}_$nodeName"

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

  "ClusterListener" must {
    "successfully set up a cluster with a potentially failing node" in {

      runOn(node1, node2, node3) {
        system.actorOf(Props(new NodeActorGuardianForTestWithFailingCoordinator(probe.ref)), name = s"guardian_${nodeName}_$nodeName")
      }

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
      val readCoordinator = system.actorSelection(readCoordinatorPath(nodeName))

      readCoordinator ! correctStatement

      expectMsgType[SelectStatementExecuted]

      readCoordinator ! "GetVolatileId"

      val volatileId = expectMsgType[String]

      readCoordinator ! errorStatement

      probe.expectMsg("Failure")
      probe.expectNoMessage(5 seconds)


      expectNoMessage(5 seconds)

      readCoordinator ! correctStatement

      expectMsgType[SelectStatementExecuted]

      readCoordinator ! "GetVolatileId"

      expectMsgType[String] shouldBe volatileId
    }

    "handle errors and terminate child actor when resumes exceeds the limits" in {
        system.actorSelection(readCoordinatorPath(nodeName)).resolveOne(1 seconds).onComplete {
          case Success(readCoordinator) =>
            readCoordinator ! errorStatement
            readCoordinator ! errorStatement
            readCoordinator ! errorStatement

            probe.expectMsg("Failure")
            probe.expectMsg("Failure")
            probe.expectMsg("Failure")

            val terminationPrrobe = TestProbe()
            terminationPrrobe.watch(readCoordinator)
            terminationPrrobe.expectTerminated(readCoordinator)
          case Failure(ex) => fail(ex)
        }
    }

  }

}