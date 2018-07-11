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

import scala.concurrent.duration._

object ReplicatedSchemaCache {

  /**
    * Cache key for a shard location
    * @param db location db.
    * @param namespace location namespace.
    * @param metric location metric.
    */
  case class SchemaKey(db: String, namespace: String, metric: String)

  final case class PutSchemaInCache(key: SchemaKey, value: Schema)
  final case class SchemaCached(key: SchemaKey, value: Option[Schema])

  final case class GetSchemaFromCache(key: SchemaKey)
  final case class EvictSchema(key: SchemaKey)

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
    case PutSchemaInCache(key, value) =>
      (replicator ? Update(schemaKey(key), LWWMap(), WriteMajority(writeDuration))(_ + (key -> value)))
        .map {
          case UpdateSuccess(_, _) =>
            SchemaCached(key, Some(value))
          case _ => SchemaCached(key, None)
        }
        .pipeTo(sender())
    case EvictSchema(key) =>
      (replicator ? Update(schemaKey(key), LWWMap(), WriteMajority(writeDuration))(_ - key))
        .map(_ => SchemaCached(key, None))
        .pipeTo(sender)
    case GetSchemaFromCache(key) =>
      log.debug("searching for key {} in cache", key)
      replicator ! Get(schemaKey(key), ReadLocal, Some(SchemaRequest(key, sender())))
    case g @ GetSuccess(LWWMapKey(_), Some(SchemaRequest(key, replyTo))) =>
      g.dataValue.asInstanceOf[LWWMap[SchemaKey, Schema]].get(key) match {
        case Some(value) => replyTo ! SchemaCached(key, Some(value))
        case None        => replyTo ! SchemaCached(key, None)
      }
    case NotFound(_, Some(SchemaRequest(key, replyTo))) =>
      replyTo ! SchemaCached(key, None)
    case msg: UpdateResponse[_] =>
      log.debug("received not handled update message {}", msg)
  }
}
