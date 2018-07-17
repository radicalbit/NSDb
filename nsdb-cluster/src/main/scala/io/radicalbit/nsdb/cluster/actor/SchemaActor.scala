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

import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import io.radicalbit.nsdb.cluster.actor.SchemaActor.SchemaWarmUp
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.commands.WarmUpSchemas
import io.radicalbit.nsdb.cluster.extension.RemoteAddress
import io.radicalbit.nsdb.cluster.util.FileUtils
import io.radicalbit.nsdb.index.SchemaIndex
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{DeleteNamespace, DeleteSchema, GetSchema, UpdateSchema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{NamespaceDeleted, SchemaDeleted, SchemaGot, SchemaUpdated}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.MMapDirectory

import scala.collection.mutable

/**
  * Actor responsible of storing metric's schemas into a persistent index.
  * A [[SchemaActor]] must be created for each node of the cluster.
  *
  * @param basePath index base path.
  */
class SchemaActor(val basePath: String, schemaCoordinator: ActorRef) extends Actor with ActorLogging {

  lazy val schemaIndexes: mutable.Map[(String, String), SchemaIndex] = mutable.Map.empty

  val remoteAddress = RemoteAddress(context.system)

  private def getOrCreateSchemaIndex(db: String, namespace: String): SchemaIndex =
    schemaIndexes.getOrElse(
      (db, namespace), {
        val newIndex = new SchemaIndex(new MMapDirectory(Paths.get(basePath, db, namespace, "schemas")))
        schemaIndexes += ((db, namespace) -> newIndex)
        newIndex
      }
    )

  override def preStart(): Unit = {
    val allSchemas = FileUtils.getSubDirs(basePath).flatMap { db =>
      FileUtils.getSubDirs(db).toList.map { namespace =>
        SchemaWarmUp(db.getName, namespace.getName, getOrCreateSchemaIndex(db.getName, namespace.getName).all)
      }
    }

    schemaCoordinator ! WarmUpSchemas(allSchemas)

    log.debug("schema actor started at {}/{}", remoteAddress.address, self.path.name)
  }

  override def receive: Receive = {

    case GetSchema(db, namespace, metric) =>
      val schema = getOrCreateSchemaIndex(db, namespace).getSchema(metric)
      sender ! SchemaGot(db, namespace, metric, schema)

    case UpdateSchema(db, namespace, metric, newSchema) =>
      val index                        = getOrCreateSchemaIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.update(metric, newSchema)
      writer.close()
      index.refresh()
      sender ! SchemaUpdated(db, namespace, metric, newSchema)

    case DeleteSchema(db, namespace, metric) =>
      val index                        = getOrCreateSchemaIndex(db, namespace)
      implicit val writer: IndexWriter = index.getWriter
      index.deleteMetricSchema(metric)
      writer.close()
      index.refresh()
      sender ! SchemaDeleted(db, namespace, metric)

    case DeleteNamespace(db, namespace) =>
      val locationIndex                    = getOrCreateSchemaIndex(db, namespace)
      val locationIndexWriter: IndexWriter = locationIndex.getWriter
      locationIndex.deleteAll()(locationIndexWriter)
      locationIndexWriter.close()
      locationIndex.refresh()

      sender ! NamespaceDeleted(db, namespace)

    case SubscribeAck(Subscribe("schema", None, _)) =>
      log.debug("subscribed to topic metadata")
  }
}

object SchemaActor {

  def props(basePath: String, schemaCoordinator: ActorRef): Props =
    Props(new SchemaActor(basePath, schemaCoordinator))

  case class SchemaWarmUp(db: String, namespace: String, metricsSchemas: Seq[Schema])
}
