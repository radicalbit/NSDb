package io.radicalbit.nsdb.index

import java.nio.file.Paths

import akka.actor.{Actor, Props}
import io.radicalbit.index.BoundedIndex
import io.radicalbit.nsdb.model.Record
import org.apache.lucene.store.FSDirectory

class IndexerActor(basePath: String) extends Actor {
  import io.radicalbit.nsdb.index.IndexerActor._

  import scala.collection.mutable

  private val indexes: mutable.Map[String, BoundedIndex] = mutable.Map.empty

  private def getIndex(metric: String) =
    indexes.getOrElse(metric, {
      val path     = FSDirectory.open(Paths.get(basePath, metric))
      val newIndex = new BoundedIndex(path)
      indexes + (metric -> newIndex)
      newIndex
    })

  override def receive: Receive = {
    case AddRecord(metric, record) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      index.write(record)
      writer.flush()
      writer.close()
      sender ! RecordAdded(metric, record)
    case DeleteRecord(metric, record) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      index.delete(record)
      writer.flush()
      writer.close()
      sender ! RecordDeleted(metric, record)
    case DeleteMetric(metric) =>
      val index           = getIndex(metric)
      implicit val writer = index.getWriter
      index.deleteAll()
      writer.close()
      sender ! MetricDeleted(metric)
    case GetCount(metric) =>
      val index = getIndex(metric)
      val hits  = index.timeRange(0, Long.MaxValue)
      sender ! CountGot(metric, hits.size)
  }
}

object IndexerActor {

  def props(basePath: String): Props = Props(new IndexerActor(basePath))

  case class AddRecord(metric: String, record: Record)
  case class DeleteRecord(metric: String, record: Record)
  case class DeleteMetric(metric: String)
  case class GetCount(metric: String)
  case class CountGot(metric: String, count: Int)
  case class RecordAdded(metric: String, record: Record)
  case class RecordRejected(metric: String, record: Record, reason: String)
  case class RecordDeleted(metric: String, record: Record)
  case class MetricDeleted(metric: String)
}
