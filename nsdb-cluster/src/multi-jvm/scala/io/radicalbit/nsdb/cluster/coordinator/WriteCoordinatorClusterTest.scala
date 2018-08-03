//package io.radicalbit.nsdb.cluster.coordinator
//
//import java.util.concurrent.TimeUnit
//
//import akka.actor.{ActorRef, Props}
//import akka.cluster.pubsub.DistributedPubSub
//import akka.cluster.{Cluster, MemberStatus}
//import akka.pattern.ask
//import akka.remote.testconductor.RoleName
//import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
//import akka.testkit.ImplicitSender
//import akka.util.Timeout
//import com.typesafe.config.ConfigFactory
//import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian.{GetMetadataCache, GetSchemaCache}
//import io.radicalbit.nsdb.cluster.actor.{ClusterListener, DatabaseActorsGuardian}
//import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.GetLocations
//import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.LocationsGot
//import io.radicalbit.nsdb.common.protocol.Bit
//import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetConnectedDataNodes, MapInput}
//import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{ConnectedDataNodesGot, InputMapped}
//import io.radicalbit.rtsae.STMultiNodeSpec
//import org.scalatest.BeforeAndAfterAll
//
//import scala.concurrent.Await
//import scala.concurrent.duration._
//
//object WriteCoordinatorClusterTest extends MultiNodeConfig {
//  val node1 = role("node-1")
//  val node2 = role("node-2")
//
//  nodeConfig(node1)(ConfigFactory.parseString("""
//      |akka.remote.netty.tcp.port=2552
//    """.stripMargin))
//
//  nodeConfig(node2)(ConfigFactory.parseString("""
//      |akka.remote.netty.tcp.port=2553
//    """.stripMargin))
//
//  commonConfig(ConfigFactory.parseString("""
//    |akka.loglevel = INFO
//    |akka.actor{
//    | provider = "cluster"
//    | control-aware-dispatcher {
//    |     mailbox-type = "akka.dispatch.UnboundedControlAwareMailbox"
//    |   }
//    |}
//    |akka.log-dead-letters-during-shutdown = off
//    |nsdb{
//    |
//    |  index {
//    |    base-path= "target/test_index/WriteCoordinatorClusterTest"
//    |  }
//    |
//    |  read-coordinator.timeout = 10 seconds
//    |  namespace-schema.timeout = 10 seconds
//    |  namespace-data.timeout = 10 seconds
//    |  publisher.timeout = 10 seconds
//    |  publisher.scheduler.interval = 5 seconds
//    |  write.scheduler.interval = 5 seconds
//    |
//    |  sharding {
//    |    interval = 1m
//    |  }
//    |
//    |  read {
//    |    parallelism {
//    |      initial-size = 1
//    |      lower-bound= 1
//    |      upper-bound = 1
//    |    }
//    |  }
//    |
//    |  index.base-path = "target/test_index/WriteCoordinatorTest"
//    |  write-coordinator.timeout = 10 seconds
//    |  metadata-coordinator.timeout = 10 seconds
//    |  commit-log {
//    |    enabled = false
//    |  }
//    |}""".stripMargin))
//}
//
//class WriteCoordinatorClusterTestMultiJvmNode1 extends WriteCoordinatorClusterTest
//
//class WriteCoordinatorClusterTestMultiJvmNode2 extends WriteCoordinatorClusterTest
//
//class WriteCoordinatorClusterTest
//    extends MultiNodeSpec(WriteCoordinatorClusterTest)
//    with STMultiNodeSpec
//    with ImplicitSender
//    with BeforeAndAfterAll {
//
//  import WriteCoordinatorClusterTest._
//
//  override def initialParticipants = roles.size
//
//  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
//
//  val cluster = Cluster(system)
//
//  val guardian = system.actorOf(Props[DatabaseActorsGuardian], "databaseActorGuardian")
//
//  lazy val metadataCache = Await.result((guardian ? GetMetadataCache).mapTo[ActorRef], 5.seconds)
//  lazy val schemaCache   = Await.result((guardian ? GetSchemaCache).mapTo[ActorRef], 5.seconds)
//
//  system.actorOf(ClusterListener.props(metadataCache, schemaCache), name = "clusterListener")
//
//  val mediator = DistributedPubSub(system).mediator
//
//  def join(from: RoleName, to: RoleName): Unit = {
//    runOn(from) {
//      cluster join node(to).address
//    }
//    enterBarrier(from.name + "-joined")
//  }
//
//  "WriteCoordinator" should {
//
//    "join cluster" in within(10.seconds) {
//
//      join(node1, node1)
//      join(node2, node1)
//
//      awaitAssert {
//        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe 2
//      }
//
//      enterBarrier("Joined")
//
//      Thread.sleep(2000)
//
//      val nodeName = s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"
//
//      awaitAssert {
//        val writeCoordinator = Await.result(
//          system.actorSelection(s"/user/guardian_$nodeName/write-coordinator_$nodeName").resolveOne(1 seconds),
//          1 seconds)
//        writeCoordinator ! GetConnectedDataNodes
//        expectMsgType[ConnectedDataNodesGot].nodes.size shouldBe 2
//      }
//
//      enterBarrier("Connected")
//    }
//
//    "write records and update metadata" in within(10.seconds) {
//
//      val selfMember = cluster.selfMember
//      val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//      awaitAssert {
//        val metadataCoordinator = Await.result(
//          system.actorSelection(s"/user/guardian_$nodeName/metadata-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        metadataCoordinator ! GetLocations("db", "namespace", "metric")
//        val initialLoc = expectMsgType[LocationsGot]
//        initialLoc.locations.size shouldBe 0
//      }
//
//      enterBarrier("Clean startup")
//
//      runOn(node1) {
//
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val writeCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/write-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(0, 1.0, Map.empty, Map.empty))
//        expectMsgType[InputMapped]
//      }
//
//      awaitAssert {
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val metadataCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        metadataCoordinator ! GetLocations("db", "namespace", "metric")
//        val locations_1 = expectMsgType[LocationsGot]
//        locations_1.locations.size shouldBe 1
//        locations_1.locations.head.from shouldBe 0
//        locations_1.locations.head.to shouldBe 60000
//      }
//
//      enterBarrier("single location from node 1")
//
//      runOn(node2) {
//        awaitAssert {
//          val selfMember = cluster.selfMember
//          val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//          val writeCoordinator = Await.result(
//            system.actorSelection(s"user/guardian_$nodeName/write-coordinator_$nodeName").resolveOne(5 seconds),
//            5 seconds)
//
//          writeCoordinator ! MapInput(1, "db", "namespace", "metric", Bit(1, 1.0, Map.empty, Map.empty))
//          expectMsgType[InputMapped]
//        }
//      }
//
//      awaitAssert {
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val metadataCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        metadataCoordinator ! GetLocations("db", "namespace", "metric")
//        val locations_2 = expectMsgType[LocationsGot]
//        locations_2.locations.size shouldBe 1
//        locations_2.locations.head.from shouldBe 0
//        locations_2.locations.head.to shouldBe 60000
//      }
//
//      enterBarrier("single location from node 2")
//
//      runOn(node2) {
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val writeCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/write-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(50000, 1.0, Map.empty, Map.empty))
//        expectMsgType[InputMapped]
//      }
//
//      awaitAssert {
//
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val metadataCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        metadataCoordinator ! GetLocations("db", "namespace", "metric")
//
//        val locations_3 = expectMsgType[LocationsGot]
//        locations_3.locations.size shouldBe 1
//        locations_3.locations.head.from shouldBe 0
//        locations_3.locations.head.to shouldBe 60000
//      }
//
//      runOn(node1) {
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val writeCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/write-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(30000, 1.0, Map.empty, Map.empty))
//        expectMsgType[InputMapped]
//      }
//      runOn(node2) {
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val writeCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/write-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        writeCoordinator ! MapInput(0, "db", "namespace", "metric", Bit(40000, 1.0, Map.empty, Map.empty))
//        expectMsgType[InputMapped]
//      }
//
//      awaitAssert {
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val metadataCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        metadataCoordinator ! GetLocations("db", "namespace", "metric")
//
//        val locations_4 = expectMsgType[LocationsGot]
//        locations_4.locations.size shouldBe 1
//        locations_4.locations.head.from shouldBe 0
//        locations_4.locations.head.to shouldBe 60000
//      }
//
//      enterBarrier("Single Location")
//
//      runOn(node1) {
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val writeCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/write-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        writeCoordinator ! MapInput(60001, "db", "namespace", "metric", Bit(60001, 1.0, Map.empty, Map.empty))
//        expectMsgType[InputMapped]
//      }
//
//      awaitAssert {
//
//        val selfMember = cluster.selfMember
//        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"
//
//        val metadataCoordinator = Await.result(
//          system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName").resolveOne(5 seconds),
//          5 seconds)
//
//        metadataCoordinator ! GetLocations("db", "namespace", "metric")
//
//        val locations_5 = expectMsgType[LocationsGot]
//        locations_5.locations.size shouldBe 2
//        locations_5.locations.head.from shouldBe 0
//        locations_5.locations.head.to shouldBe 60000
//        locations_5.locations.last.from shouldBe 60000
//        locations_5.locations.last.to shouldBe 120000
//      }
//
//      enterBarrier("Multiple Location")
//
//    }
//
//  }
//}