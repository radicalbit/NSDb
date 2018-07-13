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

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ddata._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.ReplicatedSchemaCache._
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{EvictSchema, GetSchemaFromCache, PutSchemaInCache}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SchemaCached

import scala.concurrent.duration._

object ReplicatedSchemaCache {

  /**
    * Cache key for a shard location
    * @param db location db.
    * @param namespace location namespace.
    * @param metric location metric.
    */
  case class SchemaKey(db: String, namespace: String, metric: String)

  private final case class SchemaRequest(key: SchemaKey, replyTo: ActorRef)
}

/**
  * cluster aware cache to store metric's locations based on [[akka.cluster.ddata.Replicator]]
  */
class ReplicatedSchemaCache extends Actor with ActorLogging {

  import akka.cluster.ddata.Replicator._

  implicit val cluster: Cluster = Cluster(context.system)

  val replicator: ActorRef = DistributedData(context.system).replicator

  /**
    * convert a [[SchemaKey]] into an internal cache key
    * @param schemaKey the location key to convert
    * @return [[LWWMapKey]] resulted from schemaKey hashCode
    */
  private def schemaKey(schemaKey: SchemaKey): LWWMapKey[SchemaKey, Schema] =
    LWWMapKey("schema-cache-" + math.abs(schemaKey.hashCode) % 100)

  private val writeDuration = 5.seconds

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  def receive: Receive = {
    case PutSchemaInCache(db, namespace, metric, value) =>
      val key = SchemaKey(db, namespace, metric)
      (replicator ? Update(schemaKey(key), LWWMap(), WriteMajority(writeDuration))(_ + (key -> value)))
        .map {
          case UpdateSuccess(_, _) =>
            SchemaCached(db, namespace, metric, Some(value))
          case _ => SchemaCached(db, namespace, metric, None)
        }
        .pipeTo(sender())
    case EvictSchema(db, namespace, metric) =>
      val key = SchemaKey(db, namespace, metric)
      (replicator ? Update(schemaKey(key), LWWMap(), WriteMajority(writeDuration))(_ - key))
        .map(_ => SchemaCached(db, namespace, metric, None))
        .pipeTo(sender)
    case GetSchemaFromCache(db, namespace, metric) =>
      val key = SchemaKey(db, namespace, metric)
      log.debug("searching for key {} in cache", key)
      replicator ! Get(schemaKey(key), ReadLocal, Some(SchemaRequest(key, sender())))
    case g @ GetSuccess(LWWMapKey(_), Some(SchemaRequest(key, replyTo))) =>
      val (db: String, namespace: String, metric: String) = SchemaKey.unapply(key).get
      g.dataValue.asInstanceOf[LWWMap[SchemaKey, Schema]].get(key) match {
        case Some(value) => replyTo ! SchemaCached(db, namespace, metric, Some(value))
        case None        => replyTo ! SchemaCached(db, namespace, metric, None)
      }
    case NotFound(_, Some(SchemaRequest(key, replyTo))) =>
      val (db: String, namespace: String, metric: String) = SchemaKey.unapply(key).get
      replyTo ! SchemaCached(db, namespace, metric, None)
    case msg: UpdateResponse[_] =>
      log.debug("received not handled update message {}", msg)
  }
}
