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

package io.radicalbit.nsdb.actors

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.ShardAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.ShardPerformerActor.PerformShardWrites
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter
import org.apache.lucene.index.IndexWriter

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

/**
  * Actor responsible for performing write accumulated by [[ShardAccumulatorActor]] into shards indexes.
  *
  * @param basePath shards indexes base path.
  * @param db shards db.
  * @param namespace shards namespace.
  */
class ShardPerformerActor(val basePath: String, val db: String, val namespace: String)
    extends Actor
    with ShardsActor
    with ActorLogging {

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

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
