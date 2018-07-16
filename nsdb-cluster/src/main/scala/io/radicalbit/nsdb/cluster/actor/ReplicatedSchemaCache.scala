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

import akka.actor.ActorRef
import akka.cluster.Cluster
import akka.cluster.ddata._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.ReplicatedSchemaCache._
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{NamespaceDeleted, SchemaCached}
import io.radicalbit.nsdb.util.ActorPathLogging

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

  private final case class NamespaceRequest(db: String, namespace: String, replyTo: ActorRef)
}

/**
  * cluster aware cache to store metric's locations based on [[akka.cluster.ddata.Replicator]]
  */
class ReplicatedSchemaCache extends ActorPathLogging {

  import akka.cluster.ddata.Replicator._

  implicit val cluster: Cluster = Cluster(context.system)

  val replicator: ActorRef = DistributedData(context.system).replicator

  /**
    * convert a db and a namespace into an internal cache key
    * @param db the db.
    * @param namespace the namespace.
    * @return [[LWWMapKey]] resulted from namespaceKey hashCode
    */
  private def namespaceKey(db: String, namespace: String): LWWMapKey[SchemaKey, Schema] =
    LWWMapKey(s"schema-cache-${math.abs((db + namespace).hashCode) % 100}")

  private val writeDuration = 5.seconds

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  def receive: Receive = {
    case PutSchemaInCache(db, namespace, metric, value) =>
      val key = SchemaKey(db, namespace, metric)
      (replicator ? Update(namespaceKey(db, namespace), LWWMap(), WriteAll(writeDuration))(_ + (key -> value)))
        .map {
          case UpdateSuccess(_, _) =>
            SchemaCached(db, namespace, metric, Some(value))
          case _ => SchemaCached(db, namespace, metric, None)
        }
        .pipeTo(sender())
    case EvictSchema(db, namespace, metric) =>
      val key = SchemaKey(db, namespace, metric)
      (replicator ? Update(namespaceKey(db, namespace), LWWMap(), WriteAll(writeDuration))(_ - key))
        .map(_ => SchemaCached(db, namespace, metric, None))
        .pipeTo(sender)
    case DeleteNamespace(db, namespace) =>
      replicator ! Delete(namespaceKey(db, namespace),
                          WriteAll(writeDuration),
                          Some(NamespaceRequest(db, namespace, sender())))
    case GetSchemaFromCache(db, namespace, metric) =>
      val key = SchemaKey(db, namespace, metric)
      replicator ! Get(namespaceKey(db, namespace), ReadLocal, Some(SchemaRequest(key, sender())))
    case g @ GetSuccess(LWWMapKey(_), Some(SchemaRequest(key, replyTo))) =>
      val (db: String, namespace: String, metric: String) = SchemaKey.unapply(key).get
      g.dataValue.asInstanceOf[LWWMap[SchemaKey, Schema]].get(key) match {
        case Some(value) => replyTo ! SchemaCached(db, namespace, metric, Some(value))
        case None        => replyTo ! SchemaCached(db, namespace, metric, None)
      }
    case NotFound(_, Some(SchemaRequest(key, replyTo))) =>
      val (db: String, namespace: String, metric: String) = SchemaKey.unapply(key).get
      replyTo ! SchemaCached(db, namespace, metric, None)
    //any request against a deleted cache entry will produce this message
    case DataDeleted(_, Some(SchemaRequest(key, replyTo))) =>
      val (db: String, namespace: String, metric: String) = SchemaKey.unapply(key).get
      replyTo ! SchemaCached(db, namespace, metric, None)
    case DeleteSuccess(_, Some(NamespaceRequest(db, namespace, replyTo))) =>
      replyTo ! NamespaceDeleted(db, namespace)
    case msg =>
      log.error("------------------received not handled update message {}", msg)
  }
}
