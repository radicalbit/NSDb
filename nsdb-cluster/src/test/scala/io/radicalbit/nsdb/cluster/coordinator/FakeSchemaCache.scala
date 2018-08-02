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

package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.Actor
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.commands.DeleteNamespaceSchema
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.events.NamespaceSchemaDeleted
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{EvictSchema, GetSchemaFromCache, PutSchemaInCache}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SchemaCached

import scala.collection.mutable

class FakeSchemaCache extends Actor {

  val schemas: mutable.Map[(String, String, String), Schema] = mutable.Map.empty

  def receive: Receive = {
    case PutSchemaInCache(db, namespace, metric, value) =>
      schemas.put((db, namespace, metric), value)
      sender ! SchemaCached(db, namespace, metric, Some(value))
    case GetSchemaFromCache(db, namespace, metric) =>
      sender ! SchemaCached(db, namespace, metric, schemas.get((db, namespace, metric)))
    case EvictSchema(db, namespace, metric) =>
      schemas -= ((db, namespace, metric))
      sender ! SchemaCached(db, namespace, metric, None)
    case DeleteNamespaceSchema(db, namespace) =>
      schemas.clear()
      sender ! NamespaceSchemaDeleted(db, namespace)

  }
}
