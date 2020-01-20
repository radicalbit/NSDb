/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.actors.MetricAccumulatorActor.Refresh
import io.radicalbit.nsdb.actors.MetricPerformerActor.{PerformRetry, PerformShardWrites, PersistedBit, PersistedBits}
import io.radicalbit.nsdb.common.exception.TooManyRetriesException
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}
import io.radicalbit.nsdb.index.AllFacetIndexes
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.statement.StatementParser
import io.radicalbit.nsdb.util.ActorPathLogging
import org.apache.lucene.index.IndexWriter

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

/**
  * Actor responsible for performing write accumulated by [[MetricAccumulatorActor]] into shards indexes.
  *
  * @param basePath shards indexes base path.
  * @param db shards db.
  * @param namespace shards namespace.
  */
class MetricPerformerActor(val basePath: String,
                           val db: String,
                           val namespace: String,
                           val localCommitLogCoordinator: ActorRef)
    extends ActorPathLogging
    with MetricsActor {

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.publisher.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  private val toRetryOperations: ListBuffer[(ShardOperation, Int)] = ListBuffer.empty

  private val maxAttempts = context.system.settings.config.getInt("nsdb.write.retry-attempts")

  def receive: Receive = {
    case PerformShardWrites(opBufferMap) =>
      val groupedByKey = opBufferMap.values.groupBy(_.location)

      val performedBitOperations: ListBuffer[PersistedBit] = ListBuffer.empty

      groupedByKey.foreach {
        case (loc, ops) =>
          val index               = getOrCreateIndex(loc)
          val facetIndexes        = getOrCreatefacetIndexesFor(loc)
          val writer: IndexWriter = index.getWriter

          val facetsIndexWriter = facetIndexes.newIndexWriter
          val facetsTaxoWriter  = facetIndexes.newDirectoryTaxonomyWriter

          ops.foreach {
            case op @ WriteShardOperation(_, _, bit) =>
              log.debug("performing write for bit {}", bit)
              index.write(bit)(writer) match {
                case Success(_) =>
                  facetIndexes.write(bit)(facetsIndexWriter, facetsTaxoWriter) match {
                    case Success(_) =>
                      val timestamp = System.currentTimeMillis()
                      performedBitOperations += PersistedBit(db, namespace, loc.metric, timestamp, bit, loc)
                    case Failure(t) =>
                      toRetryOperations += ((op, 0))
                      log.error(t, "error during write on facet indexes")
                  }

                case Failure(t) =>
                  toRetryOperations += ((op, 0))
                  log.error(t, "error during write on index")
              }
            case op @ DeleteShardRecordOperation(_, _, bit) =>
              index.delete(bit)(writer) match {
                case Success(_) =>
                  Try(facetIndexes.delete(bit)(facetsIndexWriter).map(_.get)) match {
                    case Success(_) =>
                      val timestamp = System.currentTimeMillis()
                      performedBitOperations += PersistedBit(db, namespace, loc.metric, timestamp, bit, loc)
                    case Failure(t) =>
                      toRetryOperations += ((op, 0))
                      log.error(t, "error during write on facet indexes")
                  }
                case Failure(t) =>
                  toRetryOperations += ((op, 0))
                  log.error(t, s"error during delete of Bit: $bit")
              }
            //FIXME add compensation logic here as well
            case DeleteShardQueryOperation(_, _, statement, schema) =>
              (for {
                parsedQuery      <- StatementParser.parseStatement(statement, schema)
                _                <- index.delete(parsedQuery.q)(writer)
                facetIndexResult <- facetIndexes.delete(parsedQuery.q)(facetsIndexWriter).head
              } yield facetIndexResult).recover {
                case t: Throwable =>
                  log.error(t, s"error during delete by statement $statement")
              }
          }

          writer.flush()
          writer.close()

          facetsTaxoWriter.close()
          facetsIndexWriter.close()
      }

      garbageCollectIndexes()

      val persistedBits = performedBitOperations
      context.parent ! Refresh(opBufferMap.keys.toSeq, groupedByKey.keys.toSeq)
      (localCommitLogCoordinator ? PersistedBits(persistedBits)).recover {
        case _ => localCommitLogCoordinator ! PersistedBits(persistedBits)
      }

      if (toRetryOperations.nonEmpty)
        self ! PerformRetry

    case PerformRetry =>
      toRetryOperations.foreach {
        case e @ (op, attempt) =>
          /**
            * the operation has been retried too many times. It's time to mark the node as unavailable
            */
          if (attempt >= maxAttempts) {
            throw new TooManyRetriesException(op.toString)
          }

          log.error("retrying operation {} attempt {} ", op, attempt)

          val loc                          = op.location
          val index                        = getOrCreateIndex(loc)
          val facetIndexes                 = getOrCreatefacetIndexesFor(loc)
          implicit val writer: IndexWriter = index.getWriter

          val facets            = new AllFacetIndexes(basePath = basePath, db = db, namespace = namespace, location = loc)
          val facetsIndexWriter = facets.newIndexWriter
          val facetsTaxoWriter  = facets.newDirectoryTaxonomyWriter

          /**
            * compensate the failed action
            */
          val compensation = (ShardOperation.getCompensation(op) map {
            case DeleteShardRecordOperation(_, _, bit) =>
              Try((Seq(index.delete(bit)) ++ facetIndexes.delete(bit)).map(_.get))
            case WriteShardOperation(_, _, bit) =>
              Try(Seq(index.write(bit), facets.write(bit)(facetsIndexWriter, facetsTaxoWriter)).map(_.get))
            case _ => Success(Seq(0l)) //do nothing for now. Return Success
          }).getOrElse(Success(Seq(0l)))

          compensation match {
            case Success(_) =>
              //Replay the operation itself
              op match {
                case DeleteShardRecordOperation(_, _, bit) =>
                  Try(Seq(index.delete(bit), Try(facetIndexes.delete(bit).map(_.get))).map(_.get)) match {
                    case Success(_) =>
                      toRetryOperations -= e
                    case Failure(t) =>
                      log.error(t, s"error during delete of Bit: {}", bit)
                      toRetryOperations -= e
                      toRetryOperations += ((op, attempt + 1))
                  }
                case WriteShardOperation(_, _, bit) =>
                  Try(Seq(index.write(bit), facets.write(bit)(facetsIndexWriter, facetsTaxoWriter)).map(_.get)) match {
                    case Success(_) =>
                      toRetryOperations -= e
                    case Failure(t) =>
                      log.error(t, s"error during write of Bit: {}", bit)
                      toRetryOperations -= e
                      toRetryOperations += ((op, attempt + 1))
                  }
                case _ => //do nothing for now
              }

            case Failure(t) =>
              log.error(t, s"failure in compensation of op {}", op)
              toRetryOperations -= e
              toRetryOperations += ((op, attempt + 1))
          }

          writer.flush()
          writer.close()

          facetsTaxoWriter.close()
          facetsIndexWriter.close()

      }

      if (toRetryOperations.nonEmpty)
        self ! PerformRetry
  }
}

object MetricPerformerActor {

  private case object PerformRetry extends NSDbSerializable

  case class PerformShardWrites(opBufferMap: Map[String, ShardOperation]) extends NSDbSerializable

  /**
    * This message is sent back to localWriteCoordinator in order to write on commit log related entries
    * @param persistedBits [[Seq]] of [[PersistedBit]]
    */
  case class PersistedBits(persistedBits: Seq[PersistedBit]) extends NSDbSerializable
  case class PersistedBit(db: String, namespace: String, metric: String, timestamp: Long, bit: Bit, location: Location)
      extends NSDbSerializable
  case object PersistedBitsAck extends NSDbSerializable

  def props(basePath: String, db: String, namespace: String, localCommitLogCoordinator: ActorRef): Props =
    Props(new MetricPerformerActor(basePath, db, namespace, localCommitLogCoordinator))
}
