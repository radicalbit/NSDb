package io.radicalbit.nsdb.cluster.actor

import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.extension.RemoteAddress
import io.radicalbit.nsdb.cluster.index.MetadataIndex
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.NIOFSDirectory

import scala.collection.mutable

class MetadataActor(val basePath: String, val coordinator: ActorRef) extends Actor with ActorLogging {

  lazy val metadataIndexes: mutable.Map[String, MetadataIndex] = mutable.Map.empty

  val remoteAddress = RemoteAddress(context.system)

  private def getIndex(namespace: String): MetadataIndex =
    metadataIndexes.getOrElse(
      namespace, {
        val newIndex = new MetadataIndex(new NIOFSDirectory(Paths.get(basePath, namespace, "metadata")))
        metadataIndexes += (namespace -> newIndex)
        newIndex
      }
    )

  override def preStart(): Unit = {
    log.debug("metadata actor started at {}/{}", remoteAddress.address, self.path.name)
  }

  override def receive: Receive = {

    case GetLocations(namespace, metric) =>
      val metadata = getIndex(namespace).getMetadata(metric)
      sender ! LocationsGot(namespace, metric, metadata)

    case GetLocation(namespace, metric, t) =>
      val metadata = getIndex(namespace).getMetadata(metric, t)
      sender ! LocationGot(namespace, metric, t, metadata)

    case AddLocation(namespace, metadata) =>
      val index                        = getIndex(namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.write(metadata)
      writer.close()
      sender ! LocationAdded(namespace, metadata)

    case AddLocations(namespace, metadataSeq) =>
      val index                        = getIndex(namespace)
      implicit val writer: IndexWriter = index.getWriter
      metadataSeq.foreach(index.write)
      writer.close()
      sender ! LocationsAdded(namespace, metadataSeq)

    case UpdateLocation(namespace, oldMetadata, newMetadata) =>
      val index                        = getIndex(namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.delete(oldMetadata)
      index.write(newMetadata)
      writer.close()
      sender ! LocationUpdated(namespace, oldMetadata, newMetadata)

    case DeleteLocation(namespace, metadata) =>
      val index                        = getIndex(namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.delete(metadata)
      writer.close()
      sender ! LocationDeleted(namespace, metadata)

    case DeleteNamespace(namespace) =>
      val index                        = getIndex(namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.deleteAll()
      writer.close()
      sender ! NamespaceDeleted(namespace)

    case SubscribeAck(Subscribe("metadata", None, _)) =>
      log.debug("subscribed to topic metadata")
  }
}

object MetadataActor {
  def props(basePath: String, coordinator: ActorRef): Props =
    Props(new MetadataActor(basePath, coordinator))
}
