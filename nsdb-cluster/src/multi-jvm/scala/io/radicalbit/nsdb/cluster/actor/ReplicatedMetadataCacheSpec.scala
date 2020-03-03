package io.radicalbit.nsdb.cluster.actor

import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Replicator.{GetReplicaCount, ReplicaCount}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.model.Location
import io.radicalbit.rtsae.STMultiNodeSpec

import scala.concurrent.duration._

object ReplicatedMetadataCacheSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")

  commonConfig(ConfigFactory.parseString("""
    |akka.loglevel = ERROR
    |akka.actor {
    | provider = "cluster"
    |
    | serialization-bindings {
    |   "io.radicalbit.nsdb.common.protocol.NSDbSerializable" = jackson-json
    | }
    |
    | control-aware-dispatcher {
    |     mailbox-type = "akka.dispatch.UnboundedControlAwareMailbox"
    |   }
    |}
    |akka.log-dead-letters-during-shutdown = off
    |nsdb {
    |
    |  global.timeout = 30 seconds
    |  read-coordinator.timeout = 10 seconds
    |  namespace-schema.timeout = 10 seconds
    |  namespace-data.timeout = 10 seconds
    |  publisher.timeout = 10 seconds
    |  publisher.scheduler.interval = 5 seconds
    |  write.scheduler.interval = 15 seconds
    |
    |  cluster.metadata-write-consistency = "all"
    |
    |  write-coordinator.timeout = 5 seconds
    |  metadata-coordinator.timeout = 5 seconds
    |  commit-log {
    |    serializer = "io.radicalbit.nsdb.commit_log.StandardCommitLogSerializer"
    |    writer = "io.radicalbit.nsdb.commit_log.RollingCommitLogFileWriter"
    |    directory = "target/commitLog"
    |    max-size = 50000
    |    passivate-after = 5s
    |  }
    |}
    """.stripMargin))
}

class ReplicatedMetadataCacheSpecMultiJvmNode1 extends ReplicatedMetadataCacheSpec

class ReplicatedMetadataCacheSpecMultiJvmNode2 extends ReplicatedMetadataCacheSpec

class ReplicatedMetadataCacheSpec
    extends MultiNodeSpec(ReplicatedMetadataCacheSpec)
    with STMultiNodeSpec
    with ImplicitSender {

  import ReplicatedMetadataCacheSpec._

  override def initialParticipants: Int = roles.size

  private val cluster         = Cluster(system)
  private val replicatedCache = system.actorOf(Props[ReplicatedMetadataCache])

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
        expectMsgType[ReplicaCount].n shouldBe 2
      }

      enterBarrier("joined")
    }

    "replicate cached entry" in within(5.seconds) {

      val metric    = "metric"
      val metric1    = "metric1"
      val location1 = Location(metric, "node1", 0, 1)
      val location2 = Location(metric, "node2", 0, 1)
      val location22 = Location("metric2", "node2", 0, 1)

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

      val newLocation = Location("metric2", "localhost", 0, 1)

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
      val l1 = Location("metric1", "node1", 0,1)
      val l2 = Location("metric2", "node1", 0,1)

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
      val location = Location(metric, "node1", _: Long, _: Long)

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
        cached.value.size shouldBe 11
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
      val location  = Location(metric, "node1", 0, 1)

      runOn(node1) {
        replicatedCache ! PutLocationInCache("db", "namespace", metric,  location)
        expectMsg(LocationCached("db", "namespace", metric,  location))
      }

      runOn(node2) {
        awaitAssert {
          replicatedCache ! GetLocationsFromCache("db", "namespace", metric)
          expectMsg(LocationsCached("db", "namespace", metric, Seq(location)))
        }

        replicatedCache ! EvictLocation("db", "namespace", Location(metric, "node1", 0, 1))
        expectMsg(Right(LocationEvicted("db", "namespace", Location(metric, "node1", 0, 1))))
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
      val location        = Location(metric, "node1", 0, 1)
      val updatedLocation = Location(metric, "node1", 0, 1)

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

    "evict locations for a node" in within(5.seconds) {
      val db = "db2"
      val namespace = "namespace2"
      val metric   = "metric2"

      runOn(node1) {
        for (i ← 10 to 20) {
          replicatedCache ! PutLocationInCache(db, namespace, metric, Location(metric, "node10", i - 1, i))
          expectMsg(LocationCached(db, namespace, metric, Location(metric, "node10",i - 1, i)))
        }
      }

      runOn(node2) {
        for (i ← 10 to 20) {
          replicatedCache ! PutLocationInCache(db, namespace, metric, Location(metric, "node20", i - 1, i))
          expectMsg(LocationCached(db, namespace, metric, Location(metric, "node20",i - 1, i)))
        }
      }

      enterBarrier("after-add-multiple-node-location")

      awaitAssert {
        replicatedCache ! GetLocationsFromCache(db, namespace, metric)
        expectMsgType[LocationsCached].value.size shouldBe 22
      }

      awaitAssert {
        replicatedCache ! GetLocationsInNodeFromCache(db, namespace, metric, "node10")
        expectMsgType[LocationsCached].value.size shouldBe 11
      }
      awaitAssert {
        replicatedCache ! GetLocationsInNodeFromCache(db, namespace, metric, "node20")
        expectMsgType[LocationsCached].value.size shouldBe 11
      }

      runOn(node2) {
          replicatedCache ! EvictLocationsInNode("node10")
          expectMsg(Right(LocationsInNodeEvicted("node10")))
      }

      awaitAssert {
        replicatedCache ! GetLocationsFromCache(db, namespace, metric)
        expectMsgType[LocationsCached].value.size shouldBe 11
      }

      runOn(node1) {
          replicatedCache ! EvictLocationsInNode("node20")
          expectMsg(Right(LocationsInNodeEvicted("node20")))
      }

      awaitAssert {
        replicatedCache ! GetLocationsFromCache(db, namespace, metric)
        expectMsgType[LocationsCached].value.size shouldBe 0
      }

      enterBarrier("after-node-evict")
    }

  }

}
