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
import io.radicalbit.nsdb.cluster.actor.MetadataActor.MetricLocations
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.extension.RemoteAddress
import io.radicalbit.nsdb.cluster.index.{Location, MetadataIndex}
import io.radicalbit.nsdb.cluster.util.FileUtils
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.MMapDirectory

import scala.collection.mutable

/**
  * Actor responsible of storing metric's locations into a persistent index.
  * A [[MetadataActor]] must be created for each node of the cluster.
  * @param basePath index base path.
  */
class MetadataActor(val basePath: String, metadataCoordinator: ActorRef) extends Actor with ActorLogging {

  lazy val metadataIndexes: mutable.Map[(String, String), MetadataIndex] = mutable.Map.empty

  val remoteAddress = RemoteAddress(context.system)

  private def getIndex(db: String, namespace: String): MetadataIndex =
    metadataIndexes.getOrElse(
      (db, namespace), {
        val newIndex = new MetadataIndex(new MMapDirectory(Paths.get(basePath, db, namespace, "metadata")))
        metadataIndexes += ((db, namespace) -> newIndex)
        newIndex
      }
    )

  override def preStart(): Unit = {
    val allLocations = FileUtils.getSubDirs(basePath).flatMap { db =>
      FileUtils.getSubDirs(db).toList.flatMap { namespace =>
        val metrics = FileUtils
          .getSubDirs(namespace.getPath + "/shards")
          .flatMap { metricShard =>
            metricShard.getName.split("_").headOption
          }
          .toSet
        metrics.map { metric =>
          log.debug(s"db : ${db.getName}, namespace : ${namespace.getName}, metric: $metric }")
          val locations = getIndex(db.getName, namespace.getName).getMetadata(metric)
          MetricLocations(db.getName, namespace.getName, metric, locations)
        }
      }
    }

    metadataCoordinator ! WarmUpLocations(allLocations)

    log.debug("metadata actor started at {}/{}", remoteAddress.address, self.path.name)
  }

  override def receive: Receive = {

    case GetLocations(db, namespace, metric) =>
      val metadata = getIndex(db, namespace).getMetadata(metric)
      sender ! LocationsGot(db, namespace, metric, metadata)

    case AddLocation(db, namespace, metadata) =>
      val index                        = getIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.write(metadata)
      writer.close()
      sender ! LocationAdded(db, namespace, metadata)

    case AddLocations(db, namespace, metadataSeq) =>
      val index                        = getIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      metadataSeq.foreach(index.write)
      writer.close()
      sender ! LocationsAdded(db, namespace, metadataSeq)

    case DeleteLocation(db, namespace, metadata) =>
      val index                        = getIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.delete(metadata)
      writer.close()
      sender ! LocationDeleted(db, namespace, metadata)

    case DeleteNamespace(db, namespace, occurredOn) =>
      val index                        = getIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.deleteAll()
      writer.close()
      sender ! NamespaceDeleted(db, namespace, occurredOn)

    case SubscribeAck(Subscribe("metadata", None, _)) =>
      log.debug("subscribed to topic metadata")
  }
}

object MetadataActor {

  case class MetricLocations(db: String, namespace: String, metric: String, locations: Seq[Location])

  def props(basePath: String, coordinator: ActorRef): Props =
    Props(new MetadataActor(basePath, coordinator))
}
