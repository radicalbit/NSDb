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
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.MetricPerformerActor.PerformShardWrites
import io.radicalbit.nsdb.index.AllFacetIndexes
import org.apache.lucene.index.IndexWriter

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

/**
  * Actor responsible for performing write accumulated by [[MetricAccumulatorActor]] into shards indexes.
  *
  * @param basePath shards indexes base path.
  * @param db shards db.
  * @param namespace shards namespace.
  */
class MetricPerformerActor(val basePath: String, val db: String, val namespace: String)
    extends Actor
    with MetricsActor
    with ActorLogging {

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  def receive: Receive = {
    case PerformShardWrites(opBufferMap) =>
      val groupedByKey = opBufferMap.values.groupBy(_.shardKey)
      groupedByKey.foreach {
        case (key, ops) =>
          val index                        = getIndex(key)
          val facetIndexes                 = facetIndexesFor(key)
          implicit val writer: IndexWriter = index.getWriter

          val facets            = new AllFacetIndexes(basePath = basePath, db = db, namespace = namespace, key = key)
          val facetsIndexWriter = facets.newIndexWriter
          val facetsTaxoWriter  = facets.newDirectoryTaxonomyWriter

          ops.foreach {
            case WriteShardOperation(_, _, bit) =>
              index.write(bit) match {
                case Success(_) =>
                  facets.write(bit)(facetsIndexWriter, facetsTaxoWriter) match {
                    case Success(_) =>
                    case Failure(t) =>
                      // rollback main index
                      // FIXME: we should manage here the possible delete failures
                      index.delete(bit)
                      log.error(t, "error during write on facet indexes")
                  }

                case Failure(t) => log.error(t, "error during write on index")
              }
            case DeleteShardRecordOperation(_, _, bit) =>
              index.delete(bit) match {
                case Success(_) =>
                  facetIndexes.delete(bit)
                case Failure(t) =>
                  log.error(t, s"error during delete of Bit: $bit")
              }
            case DeleteShardQueryOperation(_, _, q) =>
              index.delete(q) match {
                case Success(_) =>
                  facetIndexes.delete(q)
                case Failure(t) =>
                  log.error(t, s"error during delete by query $q")
              }
          }
          writer.flush()
          writer.close()

          facetsTaxoWriter.close()
          facetsIndexWriter.close()
      }
      context.parent ! Refresh(opBufferMap.keys.toSeq, groupedByKey.keys.toSeq)
  }
}

object MetricPerformerActor {

  case class PerformShardWrites(opBufferMap: Map[String, ShardOperation])

  def props(basePath: String, db: String, namespace: String): Props =
    Props(new MetricPerformerActor(basePath, db, namespace))
}
