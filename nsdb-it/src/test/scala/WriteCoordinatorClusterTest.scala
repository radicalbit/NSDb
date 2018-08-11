/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.concurrent.TimeUnit

import akka.cluster.{Cluster, MemberStatus}
import akka.util.Timeout
import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.minicluster.{MiniClusterStarter, NsdbMiniCluster}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class WriteCoordinatorClusterTest extends FunSuite with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  var minicluster: NsdbMiniCluster = _

  override def beforeAll(): Unit = {
    minicluster = new MiniClusterStarter(2)
    Thread.sleep(5000)
  }

  override def afterAll(): Unit = {
    minicluster.stop()
  }

  //  override protected[this] def nodesNumber: Int = 2

//  val cluster = Cluster(system)
//
//  val guardian = system.actorOf(Props[DatabaseActorsGuardian], "databaseActorGuardian")
//
//  lazy val metadataCache = Await.result((guardian ? GetMetadataCache).mapTo[ActorRef], 5.seconds)
//  lazy val schemaCache   = Await.result((guardian ? GetSchemaCache).mapTo[ActorRef], 5.seconds)
//
//  system.actorOf(ClusterListener.props(metadataCache, schemaCache), name = "clusterListener")

//  val mediator = DistributedPubSub(system).mediator
//
//  def join(from: RoleName, to: RoleName): Unit = {
//    runOn(from) {
//      cluster join node(to).address
//    }
//    enterBarrier(from.name + "-joined")
//  }

  test("join cluster") {
    assert(Cluster(minicluster.nodes.head.system).state.members.count(_.status == MemberStatus.Up) == 2)
  }

  test("add record") {
    val nsdb = Await.result(NSDB.connect(host = "127.0.0.1", port = 7817)(ExecutionContext.global), 10.seconds)

    val series = nsdb
      .db("root")
      .namespace("registry")
      .bit("people")
      .value(new java.math.BigDecimal("13"))
      .dimension("city", "Mouseton")
      .dimension("notimportant", None)
      .dimension("Someimportant", Some(2))
      .dimension("gender", "M")
      .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
      .dimension("bigDecimalDouble", new java.math.BigDecimal("12.5"))
      .dimension("OptionBigDecimal", Some(new java.math.BigDecimal("15.5")))

    val res = Await.result(nsdb.write(series), 10.seconds)

    assert(res.completedSuccessfully == true)
  }
//  "WriteCoordinator" should {
//
//    "join cluster" in {

//      join(node1, node1)
//      join(node2, node1)
//
//      awaitAssert {
//        cluster.state.members.count(_.status == MemberStatus.Up) shouldBe 2
//      }
//
//      enterBarrier("Joined")

//      Thread.sleep(2000)

//      val nodeName = s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"
//
//      awaitAssert {
//        val writeCoordinator = Await.result(
//          system.actorSelection(s"/user/guardian_$nodeName/write-coordinator_$nodeName").resolveOne(1 seconds),
//          1 seconds)
//      nodes.head.writeCoordinator ! GetConnectedDataNodes
//      expectMsgType[ConnectedDataNodesGot].nodes.size shouldBe 2
//      println("------------inside test case " + minicluster)
//      Cluster(minicluster.nodes.head.system).state.members.count(_.status == MemberStatus.up) shouldBe 2
//true shouldBe true
//    }

//    "join cluster again" in {
//      Cluster(minicluster.nodes.head.system).state.members.count(_.status == MemberStatus.up) shouldBe 2
//    }
//      enterBarrier("Connected")

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
//  }
}
