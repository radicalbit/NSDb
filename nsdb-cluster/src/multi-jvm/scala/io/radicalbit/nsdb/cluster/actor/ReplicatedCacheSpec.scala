package io.radicalbit.nsdb.cluster.actor

import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Replicator.{GetReplicaCount, ReplicaCount}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.index.{Location, MetricInfo}
import io.radicalbit.rtsae.STMultiNodeSpec
import org.json4s.DefaultFormats

import scala.concurrent.duration._

object ReplicatedMetadataCacheSpec extends MultiNodeConfig {
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
    |  index.base-path = "target/test_index/ReplicatedCacheSpec"
    |  write-coordinator.timeout = 5 seconds
    |  metadata-coordinator.timeout = 5 seconds
    |  commit-log {
    |    enabled = false
    |  }
    |}
    """.stripMargin))
}

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

      expectNoMessage(1 second)

      awaitAssert {
        DistributedData(system).replicator ! GetReplicaCount
        expectMsgType[ReplicaCount].n shouldBe 2
      }

      enterBarrier("joined")
    }

    "replicate cached entry" in within(5.seconds) {

      val metric    = "metric1"
      val key       = LocationKey("db", "namespace", metric, 0, 1)
      val location  = Location(metric, "node1", 0, 1)
      val metricKey = MetricLocationsKey("db", "namespace", metric)

      runOn(node1) {
        awaitAssert {
          replicatedCache ! PutLocationInCache(LocationKey("db", "namespace", metric, 0, 1),
                                               Location(metric, "node1", 0, 1))
          expectMsg(LocationCached(key, Some(location)))
        }
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetLocationFromCache(key)
          expectMsg(LocationCached(key, Some(location)))
        }
        awaitAssert {
          replicatedCache ! GetLocationsFromCache(metricKey)
          expectMsg(LocationsCached(metricKey, Seq(location)))
        }
      }
      enterBarrier("after-add-location")

      val metricInfoKey   = MetricInfoKey("db", "namespace", metric)
      val metricInfoValue = MetricInfo(metric, 100)

      runOn(node2) {
        awaitAssert {
          replicatedCache ! PutMetricInfoInCache(metricInfoKey, metricInfoValue)
          expectMsg(MetricInfoCached(metricInfoKey, Some(metricInfoValue)))
        }
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetMetricInfoFromCache(metricInfoKey)
          expectMsg(MetricInfoCached(metricInfoKey, Some(metricInfoValue)))
        }
      }
      enterBarrier("after-add-metric-info")

    }

    "replicate many cached entries" in within(5.seconds) {
      val metric    = "metric2"
      val key       = LocationKey("db", "namespace", metric, _: Long, _: Long)
      val location  = Location(metric, "node1", _: Long, _: Long)
      val metricKey = MetricLocationsKey("db", "namespace", metric)

      val metricInfoKey   = MetricInfoKey("db", "namespace", _: String)
      val metricInfoValue = MetricInfo(_: String, 100)

      runOn(node1) {
        for (i ← 10 to 20) {
          replicatedCache ! PutLocationInCache(key(i - 1, i), location(i - 1, i))
          expectMsg(LocationCached(key(i - 1, i), Some(location(i - 1, i))))

          replicatedCache ! PutMetricInfoInCache(metricInfoKey(s"metric_$i"), metricInfoValue(s"metric_$i"))
          expectMsg(MetricInfoCached(metricInfoKey(s"metric_$i"), Some(metricInfoValue(s"metric_$i"))))
        }
      }

      runOn(node2) {
        awaitAssert {
          for (i ← 10 to 20) {
            replicatedCache ! GetLocationFromCache(key(i - 1, i))
            expectMsg(LocationCached(key(i - 1, i), Some(location(i - 1, i))))

            replicatedCache ! GetMetricInfoFromCache(metricInfoKey(s"metric_$i"))
            expectMsg(MetricInfoCached(metricInfoKey(s"metric_$i"), Some(metricInfoValue(s"metric_$i"))))
          }
        }
        awaitAssert {
          replicatedCache ! GetLocationsFromCache(metricKey)
          val cached = expectMsgType[LocationsCached]
          cached.value.size shouldBe 11
        }
      }
      enterBarrier("after-build-add")
    }

    "do not allow insertion of an already present metric info" in within(5.seconds) {
      val metric          = "metric"
      val metricInfoKey   = MetricInfoKey("db", "namespace", metric)
      val metricInfoValue = MetricInfo(metric, 100)

      runOn(node2) {
        awaitAssert {
          replicatedCache ! PutMetricInfoInCache(metricInfoKey, metricInfoValue)
          expectMsg(MetricInfoCached(metricInfoKey, Some(metricInfoValue)))
        }
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetMetricInfoFromCache(metricInfoKey)
          expectMsg(MetricInfoCached(metricInfoKey, Some(metricInfoValue)))
        }
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! PutMetricInfoInCache(metricInfoKey, metricInfoValue)
          expectMsg(MetricInfoAlreadyExisting(metricInfoKey, metricInfoValue))
        }
      }
    }

    "replicate evicted entry" in within(5.seconds) {
      val metric    = "metric3"
      val key       = LocationKey("db", "namespace", metric, 0, 1)
      val location  = Location(metric, "node1", 0, 1)
      val metricKey = MetricLocationsKey("db", "namespace", metric)

      runOn(node1) {
        replicatedCache ! PutLocationInCache(key, location)
        expectMsg(LocationCached(key, Some(location)))
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetLocationFromCache(key)
          expectMsg(LocationCached(key, Some(location)))
        }

        replicatedCache ! Evict(key)
        expectMsg(LocationCached(key, None))
      }

      runOn(node1) {
        awaitAssert {
          val probe = TestProbe()
          replicatedCache ! GetLocationFromCache(key)
          expectMsg(LocationCached(key, None))
        }
        awaitAssert {
          replicatedCache ! GetLocationsFromCache(metricKey)
          val cached = expectMsgType[LocationsCached]
          cached.value.size shouldBe 0
        }

      }
      enterBarrier("after-eviction")
    }

    "replicate updated cached entry" in within(5.seconds) {
      val metric          = "metric4"
      val key             = LocationKey("db", "namespace", metric, 0, 1)
      val location        = Location(metric, "node1", 0, 1)
      val updatedLocation = Location(metric, "node1", 0, 1)
      val metricKey       = MetricLocationsKey("db", "namespace", metric)

      runOn(node1) {
        replicatedCache ! PutLocationInCache(key, location)
        expectMsg(LocationCached(key, Some(location)))
      }

      runOn(node2) {
        replicatedCache ! PutLocationInCache(key, updatedLocation)
        expectMsg(LocationCached(key, Some(updatedLocation)))
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetLocationFromCache(key)
          expectMsg(LocationCached(key, Some(updatedLocation)))
        }
        awaitAssert {
          replicatedCache ! GetLocationsFromCache(metricKey)
          val cached = expectMsgType[LocationsCached]
          cached.value.size shouldBe 1
        }

      }
      enterBarrier("after-update")
    }

  }

}
