package io.radicalbit.nsdb.actor

import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Replicator.{GetReplicaCount, ReplicaCount}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache.{Cached, Evict, GetFromCache, PutInCache}
import io.radicalbit.nsdb.cluster.actor.{Key, ReplicatedMetadataCache}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.rtsae.STMultiNodeSpec
import org.json4s.DefaultFormats

import scala.concurrent.duration._

object ReplicatedMetadataCacheSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = ERROR
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    nsdb.write-coordinator.timeout = 5 seconds
    """))
}

// need one concrete test class per node
class ReplicatedMetadataCacheMultiJvmNode1 extends ReplicatedMetadataCacheSpec

class ReplicatedMetadataCacheMultiJvmNode2 extends ReplicatedMetadataCacheSpec

class ReplicatedMetadataCacheSpec
    extends MultiNodeSpec(ReplicatedMetadataCacheSpec)
    with STMultiNodeSpec
    with ImplicitSender {

  import ReplicatedMetadataCacheSpec._

  implicit val formats = DefaultFormats

  override def initialParticipants = roles.size

  val cluster         = Cluster(system)
  val replicatedCache = system.actorOf(Props[ReplicatedMetadataCache])

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }

    enterBarrier(from.name + "-joined")
  }

  "ReplicatedMetadataCache" must {

    "join cluster" in within(20.seconds) {
      join(node1, node1)
      join(node2, node1)

      awaitAssert {
        DistributedData(system).replicator ! GetReplicaCount
        expectMsg(ReplicaCount(roles.size))
      }

      enterBarrier("after-1")
    }

    "replicate cached entry" in within(10.seconds) {

      val key      = Key("namespace", "metric", 0, 1)
      val location = Location("metric", "node1", 0, 1, 1)

      val probe = TestProbe()

      runOn(node1) {
        awaitAssert {

          replicatedCache.tell(PutInCache(Key("namespace", "metric", 0, 1), Location("metric", "node1", 0, 1, 1)),
                               probe.ref)
          probe.expectMsg(Cached(key, Some(location)))
        }
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache.tell(GetFromCache(key), probe.ref)
          probe.expectMsg(Cached(key, Some(location)))
        }
      }
      enterBarrier("after-add")
    }

    "replicate many cached entries" in within(10.seconds) {
      val probe    = TestProbe()
      val key      = Key("namespace", "metric", _: Long, _: Long)
      val location = Location("metric", "node1", _: Long, _: Long, _: Long)

      runOn(node1) {
        for (i ← 10 to 20) {
          replicatedCache.tell(PutInCache(key(i - 1, i), location(i - 1, i, i)), probe.ref)
          probe.expectMsg(Cached(key(i - 1, i), Some(location(i - 1, i, i))))
        }
      }

      runOn(node2) {
        awaitAssert {
          for (i ← 10 to 20) {
            replicatedCache.tell(GetFromCache(key(i - 1, i)), probe.ref)
            probe.expectMsg(Cached(key(i - 1, i), Some(location(i - 1, i, i))))
          }
        }
      }
      enterBarrier("after-build-add")
    }

    "replicate evicted entry" in within(15.seconds) {
      val probe    = TestProbe()
      val key      = Key("namespace", "metric", 0, 1)
      val location = Location("metric", "node1", 0, 1, 1)

      runOn(node1) {
        replicatedCache.tell(PutInCache(key, location), probe.ref)
        probe.expectMsg(Cached(key, Some(location)))
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache.tell(GetFromCache(key), probe.ref)
          probe.expectMsg(Cached(key, Some(location)))
        }

        replicatedCache.tell(Evict(key), probe.ref)
        probe.expectMsg(Cached(key, None))
      }

      runOn(node1) {
        awaitAssert {
          val probe = TestProbe()
          replicatedCache.tell(GetFromCache(key), probe.ref)
          probe.expectMsg(Cached(key, None))
        }

      }
      enterBarrier("after-eviction")
    }

    "replicate updated cached entry" in within(10.seconds) {
      val key             = Key("namespace", "metric", 0, 1)
      val location        = Location("metric", "node1", 0, 1, 0)
      val updatedLocation = Location("metric", "node1", 0, 1, 0)
      val probe           = TestProbe()

      runOn(node1) {
        replicatedCache.tell(PutInCache(key, location), probe.ref)
        probe.expectMsg(Cached(key, Some(location)))
      }

      runOn(node2) {
        replicatedCache.tell(PutInCache(key, updatedLocation), probe.ref)
        probe.expectMsg(Cached(key, Some(updatedLocation)))
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache.tell(GetFromCache(key), probe.ref)
          probe.expectMsg(Cached(key, Some(updatedLocation)))
        }

      }
      enterBarrier("after-update")
    }

  }

}
