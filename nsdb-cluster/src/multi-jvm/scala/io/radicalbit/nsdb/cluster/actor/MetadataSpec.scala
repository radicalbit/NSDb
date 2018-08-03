package io.radicalbit.nsdb.cluster.actor

import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.{Cluster, MemberStatus}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian.{GetMetadataCache, GetSchemaCache}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.index.{Location, MetricInfo}
import io.radicalbit.rtsae.STMultiNodeSpec

import scala.concurrent.Await
import scala.concurrent.duration._

object MetadataSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
    |akka.loglevel = ERROR
    |akka.actor{
    | provider = "cluster"
    | control-aware-dispatcher {
    |     mailbox-type = "akka.dispatch.UnboundedControlAwareMailbox"
    |   }
    |}
    |akka.log-dead-letters-during-shutdown = off
    |nsdb{
    |
    |  read-coordinator.timeout = 10 seconds
    |  namespace-schema.timeout = 10 seconds
    |  namespace-data.timeout = 10 seconds
    |  publisher.timeout = 10 seconds
    |  publisher.scheduler.interval = 5 seconds
    |  write.scheduler.interval = 15 seconds
    |
    |  sharding {
    |    interval = 1d
    |  }
    |
    |  read {
    |    parallelism {
    |      initial-size = 1
    |      lower-bound= 1
    |      upper-bound = 1
    |    }
    |  }
    |
    |  index.base-path = "target/test_index/MetadataTest"
    |  write-coordinator.timeout = 5 seconds
    |  metadata-coordinator.timeout = 5 seconds
    |  commit-log {
    |    enabled = false
    |  }
    |}
    """.stripMargin))

  nodeConfig(node1)(ConfigFactory.parseString("""
      |akka.remote.netty.tcp.port = 2552
    """.stripMargin))

  nodeConfig(node2)(ConfigFactory.parseString("""
      |akka.remote.netty.tcp.port = 2553
    """.stripMargin))

}

class MetadataSpecMultiJvmNode1 extends MetadataSpec {}

class MetadataSpecMultiJvmNode2 extends MetadataSpec {}

class MetadataSpec extends MultiNodeSpec(MetadataSpec) with STMultiNodeSpec with ImplicitSender {

  import MetadataSpec._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)

  val mediator = DistributedPubSub(system).mediator

  val guardian = system.actorOf(Props[DatabaseActorsGuardian], "guardian")

  import akka.pattern._

  implicit val timeout: Timeout = Timeout(5.seconds)

  lazy val metadataCache = Await.result((guardian ? GetMetadataCache).mapTo[ActorRef], 5.seconds)
  lazy val schemaCache   = Await.result((guardian ? GetSchemaCache).mapTo[ActorRef], 5.seconds)

  system.actorOf(ClusterListener.props(metadataCache, schemaCache), name = "clusterListener")

  val nodeName = s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {

      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "Metadata system" must {

    "join cluster" in within(10.seconds) {
      join(node1, node1)
      join(node2, node1)

      Thread.sleep(2000)

      val nNodes = cluster.state.members.count(_.status == MemberStatus.Up)
      nNodes shouldBe 2

      awaitAssert {
        Await
          .ready(
            system
              .actorSelection(s"/user/metadata_$nodeName")
              .resolveOne(5.seconds),
            5.seconds
          )
          .value
          .get
          .isSuccess shouldBe true
      }

      enterBarrier("joined")
    }

    "add location from different nodes" in within(10.seconds) {

      runOn(node1) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = Await.result(
          system.actorSelection(s"/user/guardian_$nodeName/metadata-coordinator_$nodeName").resolveOne(5.seconds),
          5.seconds)

        awaitAssert {
          metadataCoordinator ! AddLocation("db", "namespace", Location("metric", "node-1", 0, 1))
          expectMsg(LocationAdded("db", "namespace", Location("metric", "node-1", 0, 1)))
        }
      }

      awaitAssert {
        val metadataActor =
          Await.result(
            system
              .actorSelection(
                s"/user/metadata_${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}")
              .resolveOne(5.seconds),
            5.seconds
          )
        metadataActor ! GetLocations("db", "namespace", "metric")
        expectMsg(LocationsGot("db", "namespace", "metric", Seq(Location("metric", "node-1", 0, 1))))
      }

      enterBarrier("after-add-locations")
    }

    "add metric info from different nodes" in within(10.seconds) {

      val addresses  = cluster.state.members.filter(_.status == MemberStatus.Up).map(_.address)
      val metricInfo = MetricInfo("metric", 100)

      runOn(node1) {
        val selfMember = cluster.selfMember
        val nodeName   = s"${selfMember.address.host.getOrElse("noHost")}_${selfMember.address.port.getOrElse(2552)}"

        val metadataCoordinator = system.actorSelection(s"user/guardian_$nodeName/metadata-coordinator_$nodeName")

        awaitAssert {
          metadataCoordinator ! PutMetricInfo("db", "namespace", metricInfo)
          expectMsg(MetricInfoPut("db", "namespace", metricInfo))
        }
      }

      awaitAssert {
        val metadataActor =
          system.actorSelection(
            s"user/metadata_${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}")
        metadataActor ! GetMetricInfo("db", "namespace", "metric")
        expectMsg(MetricInfoGot("db", "namespace", Some(metricInfo)))
      }

      enterBarrier("after-add-metrics-info")
    }
  }
}
