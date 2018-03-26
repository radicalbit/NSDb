package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.ShardAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.ShardPerformerActor.PerformShardWrites
import io.radicalbit.nsdb.index.{FacetIndex, TimeSeriesIndex}
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.MMapDirectory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class ShardPerformerActor(basePath: String, db: String, namespace: String) extends Actor with ActorLogging {
  import scala.collection.mutable

  val shards: mutable.Map[ShardKey, TimeSeriesIndex] = mutable.Map.empty

  val facetIndexShards: mutable.Map[ShardKey, FacetIndex] = mutable.Map.empty

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  lazy val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  private def getIndex(key: ShardKey) =
    shards.getOrElse(
      key, {
        val directory =
          new MMapDirectory(Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}"))
        val newIndex = new TimeSeriesIndex(directory)
        shards += (key -> newIndex)
        newIndex
      }
    )

  private def getFacetIndex(key: ShardKey) =
    facetIndexShards.getOrElse(
      key, {
        val directory =
          new MMapDirectory(
            Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}", "facet"))
        val taxoDirectory = new MMapDirectory(
          Paths.get(basePath, db, namespace, "shards", s"${key.metric}_${key.from}_${key.to}", "facet", "taxo"))
        val newIndex = new FacetIndex(directory, taxoDirectory)
        facetIndexShards += (key -> newIndex)
        newIndex
      }
    )

  def receive: Receive = {
    case PerformShardWrites(opBufferMap) =>
      val groupedByKey = opBufferMap.values.groupBy(_.shardKey)
      groupedByKey.foreach {
        case (key, ops) =>
          val index                               = getIndex(key)
          val facetIndex                          = getFacetIndex(key)
          implicit val writer: IndexWriter        = index.getWriter
          val facetWriter: IndexWriter            = facetIndex.getWriter
          val taxoWriter: DirectoryTaxonomyWriter = facetIndex.getTaxoWriter
          ops.foreach {
            case WriteShardOperation(_, _, bit) =>
              index.write(bit) match {
                case Success(_) =>
                  facetIndex.write(bit)(facetWriter, taxoWriter) match {
                    case Success(_) =>
                    case Failure(t) =>
                      log.error(t, "error during write on facetIndex")
                      index.delete(bit)
                  }
                case Failure(t) => log.error(t, "error during write on index")
              }
            case DeleteShardRecordOperation(_, _, bit) =>
              index.delete(bit) match {
                case Success(_) =>
                  facetIndex.delete(bit)(facetWriter)
                case Failure(t) =>
                  log.error(t, s"error during delete of Bit: $bit")
              }
            case DeleteShardQueryOperation(_, _, q) =>
              index.delete(q) match {
                case Success(_) =>
                  facetIndex.delete(q)(facetWriter)
                case Failure(t) =>
                  log.error(t, s"error during delete by query $q")
              }
          }
          writer.flush()
          writer.close()
          taxoWriter.close()
          facetWriter.close()
      }
      context.parent ! Refresh(opBufferMap.keys.toSeq, groupedByKey.keys.toSeq)
  }
}

object ShardPerformerActor {

  case class PerformShardWrites(opBufferMap: Map[String, ShardOperation])

  def props(basePath: String, db: String, namespace: String): Props =
    Props(new ShardPerformerActor(basePath, db, namespace))
}
