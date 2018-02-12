package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.nsdb.actors.IndexAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.IndexPerformerActor.PerformWrites
import io.radicalbit.nsdb.index.{FacetIndex, TimeSeriesIndex}
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.MMapDirectory

import scala.concurrent.ExecutionContextExecutor

class IndexPerformerActor(basePath: String, db: String, namespace: String, accumulatorActor: Option[ActorRef])
    extends Actor
    with ActorLogging {
  import scala.collection.mutable

  private val indexes: mutable.Map[String, TimeSeriesIndex] = mutable.Map.empty

  private val facetIndexes: mutable.Map[String, FacetIndex] = mutable.Map.empty

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  private def getIndex(metric: String) =
    indexes.getOrElse(
      metric, {
        val directory = new MMapDirectory(Paths.get(basePath, db, namespace, metric))
        val newIndex  = new TimeSeriesIndex(directory)
        indexes += (metric -> newIndex)
        newIndex
      }
    )

  private def getFacetIndex(metric: String) =
    facetIndexes.getOrElse(
      metric, {
        val directory     = new MMapDirectory(Paths.get(basePath, db, namespace, metric, "facet"))
        val taxoDirectory = new MMapDirectory(Paths.get(basePath, db, namespace, metric, "facet", "taxo"))
        val newIndex      = new FacetIndex(directory, taxoDirectory)
        facetIndexes += (metric -> newIndex)
        newIndex
      }
    )

  def receive: Receive = {
    case PerformWrites(opBufferMap: Map[String, Seq[Operation]]) =>
      val groupdByMetric = opBufferMap.values.flatten.groupBy(_.metric)
      groupdByMetric.keys.foreach { metric =>
        val index                               = getIndex(metric)
        val facetIndex                          = getFacetIndex(metric)
        implicit val writer: IndexWriter        = index.getWriter
        val facetWriter: IndexWriter            = facetIndex.getWriter
        val taxoWriter: DirectoryTaxonomyWriter = facetIndex.getTaxoWriter
        groupdByMetric(metric).foreach {
          case WriteOperation(_, _, bit) =>
            index.write(bit).map(_ => facetIndex.write(bit)(facetWriter, taxoWriter)) match {
              case Valid(_)      =>
              case Invalid(errs) => log.error(errs.toList.mkString(","))
            }
          //TODO handle errors
          case DeleteRecordOperation(_, _, bit) =>
            index.delete(bit)
            facetIndex.delete(bit)(facetWriter)
          case DeleteQueryOperation(_, _, q) =>
            val index = getIndex(metric)
            index.delete(q)
        }
        writer.close()
        taxoWriter.close()
        facetWriter.close()
        facetIndex.refresh()
        index.refresh()
      }
      accumulatorActor getOrElse context.parent ! Refresh(opBufferMap.keys.toSeq)
  }
}

object IndexPerformerActor {

  case class PerformWrites(opBufferMap: Map[String, Seq[Operation]])

  def props(basePath: String, db: String, namespace: String, accumulatorActor: Option[ActorRef] = None): Props =
    Props(new IndexPerformerActor(basePath, db, namespace, accumulatorActor))
}
