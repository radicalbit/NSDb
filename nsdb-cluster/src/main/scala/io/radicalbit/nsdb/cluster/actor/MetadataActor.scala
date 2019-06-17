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

package io.radicalbit.nsdb.cluster.actor

import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import io.radicalbit.nsdb.cluster.actor.MetadataActor.MetricMetadata
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.extension.RemoteAddress
import io.radicalbit.nsdb.cluster.index.{LocationIndex, MetricInfo, MetricInfoIndex}
import io.radicalbit.nsdb.cluster.util.FileUtils
import io.radicalbit.nsdb.model.Location
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.MMapDirectory

import scala.collection.mutable

/**
  * Actor responsible of storing metric's locations into a persistent index.
  * A [[MetadataActor]] must be created for each node of the cluster.
  *
  * @param basePath index base path.
  */
class MetadataActor(val basePath: String, metadataCoordinator: ActorRef) extends Actor with ActorLogging {

  lazy val locationIndexes: mutable.Map[(String, String), LocationIndex]     = mutable.Map.empty
  lazy val metricInfoIndexes: mutable.Map[(String, String), MetricInfoIndex] = mutable.Map.empty

  lazy val metadataTopic = context.system.settings.config.getString("nsdb.cluster.pub-sub.metadata-topic")

  val remoteAddress = RemoteAddress(context.system)

  private def getLocationIndex(db: String, namespace: String): LocationIndex =
    locationIndexes.getOrElse(
      (db, namespace), {
        val newIndex = new LocationIndex(new MMapDirectory(Paths.get(basePath, db, namespace, "metadata")))
        locationIndexes += ((db, namespace) -> newIndex)
        newIndex
      }
    )

  private def getMetricInfoIndex(db: String, namespace: String): MetricInfoIndex =
    metricInfoIndexes.getOrElse(
      (db, namespace), {
        val newIndex = new MetricInfoIndex(new MMapDirectory(Paths.get(basePath, db, namespace, "metadata", "info")))
        metricInfoIndexes += ((db, namespace) -> newIndex)
        newIndex
      }
    )

  override def preStart(): Unit = {
    val allMetadata = FileUtils.getSubDirs(basePath).flatMap { db =>
      FileUtils.getSubDirs(db).toList.flatMap { namespace =>
        val metricInfos = getMetricInfoIndex(db.getName, namespace.getName).all.map(e => e.metric -> e).toMap

        val metricsWithShards = FileUtils
          .getSubDirs(namespace.getPath + "/shards")
          .flatMap { metricShard =>
            metricShard.getName.split("_").headOption
          }
          .toSet

        val metricsMetadataWithoutShards = (metricInfos -- metricsWithShards).map(e =>
          MetricMetadata(db.getName, namespace.getName, e._1, Some(e._2), Seq.empty))

        val metricsMetadataWithShards = metricsWithShards.map { metric =>
          log.debug(s"db : ${db.getName}, namespace : ${namespace.getName}, metric: $metric }")
          val locations  = getLocationIndex(db.getName, namespace.getName).getLocationsForMetric(metric)
          val metricInfo = metricInfos.get(metric)
          MetricMetadata(db.getName, namespace.getName, metric, metricInfo, locations)
        }

        metricsMetadataWithShards.toSeq ++ metricsMetadataWithoutShards
      }
    }

    metadataCoordinator ! WarmUpMetadata(allMetadata)

    log.info("metadata actor started at {}/{}", remoteAddress.address, self.path.name)
  }

  override def receive: Receive = {

    case GetLocations(db, namespace, metric) =>
      val metadata = getLocationIndex(db, namespace).getLocationsForMetric(metric)
      sender ! LocationsGot(db, namespace, metric, metadata)

    case AddLocation(db, namespace, metadata) =>
      val index                        = getLocationIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.write(metadata)
      writer.close()
      index.refresh()
      sender ! LocationAdded(db, namespace, metadata)

    case AddLocations(db, namespace, metadataSeq) =>
      val index                        = getLocationIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      metadataSeq.foreach(index.write)
      writer.close()
      index.refresh()
      sender ! LocationsAdded(db, namespace, metadataSeq)

    case DeleteLocation(db, namespace, metadata) =>
      val index                        = getLocationIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.delete(metadata)
      writer.close()
      index.refresh()
      sender ! LocationDeleted(db, namespace, metadata)

    case DeleteMetricMetadata(db, namespace, metric, occurredOn) =>
      val index                        = getLocationIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.deleteByMetric(metric)
      writer.close()
      index.refresh()

      val metricInfoIndex  = getMetricInfoIndex(db, namespace)
      val metricInfoWriter = metricInfoIndex.getWriter
      metricInfoIndex.deleteByMetric(metric)(metricInfoWriter)
      metricInfoWriter.close()
      metricInfoIndex.refresh()

      sender ! MetricMetadataDeleted(db, namespace, metric, occurredOn)
    case DeleteNamespaceMetadata(db, namespace, occurredOn) =>
      val locationIndex                    = getLocationIndex(db, namespace)
      val locationIndexwriter: IndexWriter = locationIndex.getWriter
      locationIndex.deleteAll()(locationIndexwriter)
      locationIndexwriter.close()
      locationIndex.refresh()

      val metricInfoIndex  = getMetricInfoIndex(db, namespace)
      val metricInfoWriter = metricInfoIndex.getWriter
      metricInfoIndex.deleteAll()(metricInfoWriter)
      metricInfoWriter.close()
      metricInfoIndex.refresh()

      sender ! NamespaceMetadataDeleted(db, namespace, occurredOn)
    case PutMetricInfo(db, namespace, metricInfo) =>
      val index = getMetricInfoIndex(db, namespace)
      index.getMetricInfo(metricInfo.metric) match {
        case Some(_) => sender() ! MetricInfoFailed(db, namespace, metricInfo, "metric info already exist")
        case None =>
          implicit val writer: IndexWriter = index.getWriter
          index.write(metricInfo)
          writer.close()
          index.refresh()
      }
      sender ! MetricInfoPut(db, namespace, metricInfo)

    case GetMetricInfo(db, namespace, metric) =>
      val index         = getMetricInfoIndex(db, namespace)
      val metricInfoOpt = index.getMetricInfo(metric)
      sender ! MetricInfoGot(db, namespace, metricInfoOpt)

    case SubscribeAck(Subscribe(`metadataTopic`, None, _)) =>
      log.debug("subscribed to topic metadata")
  }
}

object MetadataActor {

  case class MetricMetadata(db: String,
                            namespace: String,
                            metric: String,
                            info: Option[MetricInfo],
                            locations: Seq[Location])

  def props(basePath: String, coordinator: ActorRef): Props =
    Props(new MetadataActor(basePath, coordinator))
}
