package io.radicalbit.nsdb.cluster.actor

import akka.actor.Props
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Replicator.{GetReplicaCount, ReplicaCount}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.NSDbMultiNodeSpec
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.common.protocol.NSDbNode
import io.radicalbit.nsdb.model.{Location, LocationWithCoordinates}

import scala.concurrent.duration._

object ReplicatedMetadataCacheSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(
    ConfigFactory.parseResources("application.conf").withValue("nsdb.blacklist.ttl", ConfigValueFactory.fromAnyRef("5s"))
  )
}

class ReplicatedMetadataCacheSpecMultiJvmNode1 extends ReplicatedMetadataCacheSpec

class ReplicatedMetadataCacheSpecMultiJvmNode2 extends ReplicatedMetadataCacheSpec

class ReplicatedMetadataCacheSpec
    extends MultiNodeSpec(ReplicatedMetadataCacheSpec)
    with NSDbMultiNodeSpec
    with ImplicitSender {

  import ReplicatedMetadataCacheSpec._

  override def initialParticipants: Int = roles.size

  private val replicatedCache = system.actorOf(Props[ReplicatedMetadataCache])

  val nsdbNode1 = NSDbNode("localhost_2552", "node1", "volatile1")
  val nsdbNode2 = NSDbNode("localhost_2553", "node2", "volatile2")

  "ReplicatedMetadataCache" must {

    "join cluster" in within(20.seconds) {
      join(node1, node1)
      join(node2, node1)

      awaitAssert {
        DistributedData(system).replicator ! GetReplicaCount
        expectMsgType[ReplicaCount].n shouldBe 2
      }

      enterBarrier("joined")
    }

    "replicate cached entry" in within(5.seconds) {

      val metric    = "metric"
      val metric1    = "metric1"
      val location1 = Location(metric, nsdbNode1, 0, 1)
      val location2 = Location(metric, nsdbNode2, 0, 1)
      val location22 = Location("metric2", nsdbNode2, 0, 1)

      runOn(node1) {
        awaitAssert {
          replicatedCache ! PutLocationInCache("db", "namespace", metric,  location1)
          expectMsg(LocationCached("db", "namespace", metric, location1))
        }
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetLocationsFromCache("db", "namespace", metric)
          expectMsg(LocationsCached("db", "namespace", metric, Seq(location1)))
        }
      }

      enterBarrier("after-add-first-location")

      runOn(node1) {
        awaitAssert {
          replicatedCache ! PutLocationInCache("db", "namespace", metric,  location2)
          expectMsg(LocationCached("db", "namespace", metric,  location2))
        }
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetLocationsFromCache("db", "namespace", metric)
          expectMsg(LocationsCached("db", "namespace", metric, Seq(location1, location2)))
        }
        awaitAssert{
          replicatedCache ! GetDbsFromCache
          expectMsg(DbsFromCacheGot(Set("db")))
        }
      }

      enterBarrier("after-add-location")

      val metricInfoValue = MetricInfo("db", "namespace",metric, 100)

      runOn(node2) {
        awaitAssert {
          replicatedCache ! PutMetricInfoInCache(metricInfoValue)
          expectMsg(MetricInfoCached("db", "namespace", metric, Some(metricInfoValue)))
        }
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetMetricInfoFromCache("db", "namespace", metric)
          expectMsg(MetricInfoCached("db", "namespace", metric, Some(metricInfoValue)))
        }

        awaitAssert {
          replicatedCache ! GetAllMetricInfoWithRetention
          expectMsg(AllMetricInfoWithRetentionGot(Set()))
        }
      }
      enterBarrier("after-add-metric-info")

      runOn(node2) {
        awaitAssert {
          replicatedCache ! PutLocationInCache("db1", "namespace1", "metric2", location22)
          expectMsg(LocationCached("db1", "namespace1", "metric2", location22))
        }
      }

        awaitAssert {
          replicatedCache ! GetLocationsFromCache("db1", "namespace1", metric1)
          expectMsg(LocationsCached("db1", "namespace1", "metric1", Seq()))

          replicatedCache ! GetLocationsFromCache("db1", "namespace1", "metric2")
          expectMsg(LocationsCached("db1", "namespace1", "metric2", Seq(location22)))
        }
        awaitAssert{
          replicatedCache ! GetDbsFromCache
          expectMsg(DbsFromCacheGot(Set("db", "db1")))

          replicatedCache ! GetNamespacesFromCache("db")
          expectMsg(NamespacesFromCacheGot("db", Set("namespace")))

          replicatedCache ! GetNamespacesFromCache("db1")
          expectMsg(NamespacesFromCacheGot("db1", Set("namespace1")))

          replicatedCache ! GetMetricsFromCache("db", "namespace")
          expectMsg(MetricsFromCacheGot("db", "namespace", Set(metric)))

          replicatedCache ! GetMetricsFromCache("db1", "namespace1")
          expectMsg(MetricsFromCacheGot("db1", "namespace1", Set("metric2")))

          replicatedCache ! GetMetricsFromCache("db", "namespace1")
          expectMsg(MetricsFromCacheGot("db", "namespace1", Set.empty))

          replicatedCache ! GetMetricsFromCache("db1", "namespace")
          expectMsg(MetricsFromCacheGot("db1", "namespace", Set.empty))
        }
      enterBarrier("after-check coordinates")
    }

    "drop metrics coordinates from cache and allow reinserting them" in within(5.seconds) {

      awaitAssert {
        replicatedCache ! GetMetricsFromCache("db1", "namespace1")
        expectMsg(MetricsFromCacheGot("db1", "namespace1", Set("metric2")))
      }

      runOn(node1) {
        replicatedCache ! DropMetricFromCache("db1", "namespace1", "metric2")
        expectMsg(MetricFromCacheDropped("db1", "namespace1", "metric2"))
      }

      enterBarrier("after-drop-metric")

      awaitAssert {
        replicatedCache ! GetMetricsFromCache("db1", "namespace1")
        expectMsg(MetricsFromCacheGot("db1", "namespace1", Set()))

        replicatedCache ! GetLocationsFromCache("db1", "namespace1", "metric2")
        expectMsg(LocationsCached("db1", "namespace1", "metric2", Seq()))

        replicatedCache ! GetMetricInfoFromCache("db1", "namespace1", "metric2")
        expectMsg(MetricInfoCached("db1", "namespace1", "metric2", None))
      }

      enterBarrier("after-drop metrics")

      val newLocation = Location("metric2", nsdbNode1.copy(nodeAddress = "localhost"), 0, 1)

      runOn(node1) {
        awaitAssert {
          replicatedCache ! PutLocationInCache("db1", "namespace1", "metric2",  newLocation)
          expectMsg(LocationCached("db1", "namespace1", "metric2",  newLocation))
        }
      }

      awaitAssert {
        replicatedCache ! GetLocationsFromCache("db1", "namespace1", "metric2")
        expectMsg(LocationsCached("db1", "namespace1", "metric2", Seq(newLocation)))
      }

      enterBarrier("after-reinsertion-after-drop-metric")
    }

    "drop namespaces coordinates and allow reinserting them" in within(5.seconds) {
      val l1 = Location("metric1", nsdbNode1, 0,1)
      val l2 = Location("metric2", nsdbNode1, 0,1)

      val metric1InfoValue = MetricInfo("db1", "namespace1", "metric1", 0, 1)
      val metric2InfoValue = MetricInfo("db1", "namespace1", "metric2", 1, 0)

      runOn(node1) {
        replicatedCache ! PutLocationInCache("db1", "namespace1", "metric1",  l1)
        expectMsg(LocationCached("db1", "namespace1", "metric1",  l1))

        replicatedCache ! PutMetricInfoInCache(metric1InfoValue)
        expectMsg(MetricInfoCached("db1", "namespace1", "metric1", Some(metric1InfoValue)))
      }

      runOn(node1) {
        replicatedCache ! PutLocationInCache("db1", "namespace1", "metric2",  l2)
        expectMsg(LocationCached("db1", "namespace1", "metric2",  l2))

        replicatedCache ! PutMetricInfoInCache(metric2InfoValue)
        expectMsg(MetricInfoCached("db1", "namespace1", "metric2", Some(metric2InfoValue)))
      }

      enterBarrier("after-add-metrics")

      awaitAssert {
        replicatedCache ! GetMetricsFromCache("db1", "namespace1")
        expectMsg(MetricsFromCacheGot("db1", "namespace1", Set("metric1","metric2")))
      }

      awaitAssert {
        replicatedCache ! GetMetricInfoFromCache("db1", "namespace1", "metric1")
        expectMsg(MetricInfoCached("db1", "namespace1", "metric1", Some(metric1InfoValue)))

        replicatedCache ! GetMetricInfoFromCache("db1", "namespace1", "metric2")
        expectMsg(MetricInfoCached("db1", "namespace1", "metric2", Some(metric2InfoValue)))
      }

      enterBarrier("after-check-metrics")

      runOn(node2) {
        replicatedCache ! DropNamespaceFromCache("db1", "namespace1")
        expectMsg(NamespaceFromCacheDropped("db1", "namespace1"))
      }

      awaitAssert {
        replicatedCache ! GetMetricsFromCache("db1", "namespace1")
        expectMsg(MetricsFromCacheGot("db1", "namespace1", Set()))
      }

      awaitAssert {
        replicatedCache ! GetNamespacesFromCache("db1")
        expectMsg(NamespacesFromCacheGot("db1", Set()))
      }

      awaitAssert {
        replicatedCache ! GetLocationsFromCache("db1", "namespace1", "metric1")
        expectMsg(LocationsCached("db1", "namespace1", "metric1", Seq()))

        replicatedCache ! GetLocationsFromCache("db1", "namespace1", "metric2")
        expectMsg(LocationsCached("db1", "namespace1", "metric2", Seq()))
      }

      awaitAssert {
        replicatedCache ! GetMetricInfoFromCache("db1", "namespace1", "metric1")
        expectMsg(MetricInfoCached("db1", "namespace1", "metric1", None))

        replicatedCache ! GetMetricInfoFromCache("db1", "namespace1", "metric2")
        expectMsg(MetricInfoCached("db1", "namespace1", "metric2", None))
      }

      enterBarrier("after-drop-namespace")

      runOn(node1) {
        replicatedCache ! PutLocationInCache("db1", "namespace1", "metric1",  l1)
        expectMsg(LocationCached("db1", "namespace1", "metric1",  l1))
      }

      runOn(node1) {
        replicatedCache ! PutLocationInCache("db1", "namespace1", "metric2",  l2)
        expectMsg(LocationCached("db1", "namespace1", "metric2",  l2))
      }

      awaitAssert {
        replicatedCache ! GetMetricsFromCache("db1", "namespace1")
        expectMsg(MetricsFromCacheGot("db1", "namespace1", Set("metric1","metric2")))
      }

      enterBarrier("after-reinsertion-after-drop-namespace")
    }

    "replicate many cached entries" in within(5.seconds) {
      val metric   = "metric2"
      val location = Location(metric, nsdbNode1, _: Long, _: Long)

      val metricInfoValue = MetricInfo("db", "namespace", _: String, 100)

      runOn(node1) {
        for (i ← 10 to 20) {
          replicatedCache ! PutLocationInCache("db", "namespace", metric, location(i - 1, i))
          expectMsg(LocationCached("db", "namespace", metric, location(i - 1, i)))

          replicatedCache ! PutMetricInfoInCache(metricInfoValue(s"metric_$i"))
          expectMsg(MetricInfoCached("db", "namespace", s"metric_$i", Some(metricInfoValue(s"metric_$i"))))
        }
      }

      runOn(node2) {
        awaitAssert {
          for (i ← 10 to 20) {
            replicatedCache ! GetMetricInfoFromCache("db", "namespace", s"metric_$i")
            expectMsg(MetricInfoCached("db", "namespace", s"metric_$i", Some(metricInfoValue(s"metric_$i"))))
          }
        }
      }

      awaitAssert {
        replicatedCache ! GetLocationsFromCache("db", "namespace", metric)
        val cached = expectMsgType[LocationsCached]
        cached.locations.size shouldBe 11
      }

      enterBarrier("after-bulk-add")
    }

    "do not allow insertion of an already present metric info" in within(5.seconds) {
      val metric          = "metricInfo"
      val metricInfoKey   = MetricInfoCacheKey("db", "namespace", metric)
      val metricInfoValue = MetricInfo("db", "namespace",metric, 100)

      runOn(node2) {
        awaitAssert {
          replicatedCache ! PutMetricInfoInCache(metricInfoValue)
          expectMsg(MetricInfoCached("db", "namespace", metric, Some(metricInfoValue)))
        }
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetMetricInfoFromCache("db", "namespace", metric)
          expectMsg(MetricInfoCached("db", "namespace", metric, Some(metricInfoValue)))
        }
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! PutMetricInfoInCache(metricInfoValue)
          expectMsg(MetricInfoAlreadyExisting(metricInfoKey, metricInfoValue))
        }
      }
    }

    "replicate evicted entry" in within(5.seconds) {
      val metric    = "metric3"
      val location  = Location(metric, nsdbNode1, 0, 1)

      runOn(node1) {
        replicatedCache ! PutLocationInCache("db", "namespace", metric,  location)
        expectMsg(LocationCached("db", "namespace", metric,  location))
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetLocationsFromCache("db", "namespace", metric)
          expectMsg(LocationsCached("db", "namespace", metric, Seq(location)))
        }

        replicatedCache ! EvictLocation("db", "namespace", Location(metric, nsdbNode1, 0, 1))
        expectMsg(LocationEvicted("db", "namespace", Location(metric, nsdbNode1, 0, 1)))
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetLocationsFromCache("db", "namespace", metric)
          expectMsg(LocationsCached("db", "namespace", metric, Seq.empty))
        }

      }
      enterBarrier("after-eviction")
    }

    "replicate updated cached entry" in within(5.seconds) {
      val metric          = "metric4"
      val location        = Location(metric, nsdbNode1, 0, 1)
      val updatedLocation = Location(metric, nsdbNode1, 0, 1)

      runOn(node1) {
        replicatedCache ! PutLocationInCache("db", "namespace", metric,  location)
        expectMsg(LocationCached("db", "namespace", metric,  location))
      }

      runOn(node2) {
        replicatedCache ! PutLocationInCache("db", "namespace", metric,  updatedLocation)
        expectMsg(LocationCached("db", "namespace", metric,  updatedLocation))
      }

      runOn(node1) {
        awaitAssert {
          replicatedCache ! GetLocationsFromCache("db", "namespace", metric)
          expectMsg(LocationsCached("db", "namespace", metric, Seq(updatedLocation)))
        }

      }
      enterBarrier("after-update")
    }

    "evict locations for a node" in {
      val db = "db2"
      val namespace = "namespace2"
      val metric   = "metric2"

      val nsdbNode10 = NSDbNode("localhost_2552", "node10", "volatile10")
      val nsdbNode20 = NSDbNode("localhost_2552", "node20", "volatile20")

      runOn(node1) {
        for (i ← 10 to 20) {
          replicatedCache ! PutLocationInCache(db, namespace, metric, Location(metric, nsdbNode10, i - 1, i))
          expectMsg(LocationCached(db, namespace, metric, Location(metric, nsdbNode10,i - 1, i)))
        }
      }

      runOn(node2) {
        for (i ← 10 to 20) {
          replicatedCache ! PutLocationInCache(db, namespace, metric, Location(metric, nsdbNode20, i - 1, i))
          expectMsg(LocationCached(db, namespace, metric, Location(metric, nsdbNode20,i - 1, i)))
        }
      }

      enterBarrier("after-add-multiple-node-location")

      awaitAssert {
        replicatedCache ! GetLocationsFromCache(db, namespace, metric)
        expectMsgType[LocationsCached].locations.size shouldBe 22
      }

      awaitAssert {
        replicatedCache ! GetLocationsInNodeFromCache(db, namespace, metric, nsdbNode10.uniqueNodeId)
        expectMsgType[LocationsCached].locations.size shouldBe 11
      }
      awaitAssert {
        replicatedCache ! GetLocationsInNodeFromCache(db, namespace, metric, nsdbNode20.uniqueNodeId)
        expectMsgType[LocationsCached].locations.size shouldBe 11
      }

      runOn(node2) {
          replicatedCache ! EvictLocationsInNode(nsdbNode10.uniqueNodeId)
          expectMsg(LocationsInNodeEvicted(nsdbNode10.uniqueNodeId))
      }

      awaitAssert {
        replicatedCache ! GetLocationsFromCache(db, namespace, metric)
        expectMsgType[LocationsCached].locations.size shouldBe 11
      }

      runOn(node1) {
          replicatedCache ! EvictLocationsInNode(nsdbNode20.uniqueNodeId)
          expectMsg(LocationsInNodeEvicted(nsdbNode20.uniqueNodeId))
      }

      awaitAssert {
        replicatedCache ! GetLocationsFromCache(db, namespace, metric)
        expectMsgType[LocationsCached].locations.size shouldBe 0
      }

      enterBarrier("after-node-evict")
    }

    "manage outdated locations" in {

      replicatedCache ! GetOutdatedLocationsFromCache

      awaitAssert {
        expectMsgType[OutdatedLocationsFromCacheGot].locations.size shouldBe 0
      }

      enterBarrier("no-outdated-locations")

      val location = Location("metric", nsdbNode1,0,1)

      awaitAssert {
        replicatedCache ! AddOutdatedLocationInCache("db", "namespace", location)
        val outdatedLocationAdded = expectMsgType[OutdatedLocationInCacheAdded]
        outdatedLocationAdded.db shouldBe "db"
        outdatedLocationAdded.namespace shouldBe "namespace"
        outdatedLocationAdded.location shouldBe location
      }

      awaitAssert {
        replicatedCache ! AddOutdatedLocationInCache("db1", "namespace1", location)
        val outdatedLocationAdded = expectMsgType[OutdatedLocationInCacheAdded]
        outdatedLocationAdded.db shouldBe "db1"
        outdatedLocationAdded.namespace shouldBe "namespace1"
        outdatedLocationAdded.location shouldBe location
      }

      enterBarrier("after-add-outdated-locations")

      awaitAssert {
        replicatedCache ! GetOutdatedLocationsFromCache
        val outdatedLocationsFromCacheGot = expectMsgType[OutdatedLocationsFromCacheGot]
        outdatedLocationsFromCacheGot.locations shouldBe
          Set(
            LocationWithCoordinates("db1", "namespace1", location),
            LocationWithCoordinates("db", "namespace", location)
          )
      }
    }

    "manage nodes blacklist" in {

      replicatedCache ! GetNodesBlackListFromCache

      awaitAssert {
        expectMsgType[NodesBlackListFromCacheGot].blacklist.size shouldBe 0
      }

      enterBarrier("no-nodes-blacklist")

      val node1 = NSDbNode("address", "fs", "volatile")
      val node2 = NSDbNode("address2", "fs2", "volatile2")

      awaitAssert {
        replicatedCache ! AddNodeToBlackList(node1)
        val outdatedLocationAdded = expectMsgType[NodeToBlackListAdded]
        outdatedLocationAdded.node shouldBe node1
      }

      awaitAssert {
        replicatedCache ! AddNodeToBlackList(node2)
        val outdatedLocationAdded = expectMsgType[NodeToBlackListAdded]
        outdatedLocationAdded.node shouldBe node2
      }

      enterBarrier("after-add-nodes-in-blacklist")

      awaitAssert {
        replicatedCache ! GetNodesBlackListFromCache
        val outdatedLocationsFromCacheGot = expectMsgType[NodesBlackListFromCacheGot]
        outdatedLocationsFromCacheGot.blacklist shouldBe Set(node1, node2)
      }
    }

    "ensure that a blacklisted node is removed from the list after the configured ttl" in {
      awaitAssert {
        replicatedCache ! GetNodesBlackListFromCache
        val outdatedLocationsFromCacheGot = expectMsgType[NodesBlackListFromCacheGot]
        outdatedLocationsFromCacheGot.blacklist.size shouldBe 2
      }

      awaitAssert {
        replicatedCache ! CheckBlackListTtl
        replicatedCache ! GetNodesBlackListFromCache
        val outdatedLocationsFromCacheGot = expectMsgType[NodesBlackListFromCacheGot]
        outdatedLocationsFromCacheGot.blacklist shouldBe Set()
      }
    }

  }

}
