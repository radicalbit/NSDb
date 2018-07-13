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

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.index.SchemaIndex
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.ActorPathLogging

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Actor responsible for dispatching read and write schema operations to che proper schema actor.
  * @param basePath indexes' base path.
  */
class SchemaCoordinator(basePath: String, schemaCache: ActorRef, mediator: ActorRef) extends ActorPathLogging {

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.namespace-schema.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  /**
    * Checks if a newSchema is compatible with an oldSchema. If schemas are compatible, the metric schema will be updated.
    * @param namespace schema's namespace.
    * @param metric schema's metric.
    * @param oldSchema current schema for metric
    * @param newSchema schema to be checked and updated.
    */
  private def checkAndUpdateSchema(db: String,
                                   namespace: String,
                                   metric: String,
                                   oldSchema: Schema,
                                   newSchema: Schema) =
    if (oldSchema == newSchema)
      Future(SchemaUpdated(db, namespace, metric, newSchema))
    else
      SchemaIndex.union(oldSchema, newSchema) match {
        case Success(unionSchema) =>
          (schemaCache ? PutSchemaInCache(db, namespace, metric, unionSchema))
            .map {
              case SchemaCached(_, _, _, _) =>
                mediator ! Publish("schema", UpdateSchema(db, namespace, metric, newSchema))
                SchemaUpdated(db, namespace, metric, newSchema)
              case msg => UpdateSchemaFailed(db, namespace, metric, List(s"Unknown response from schema cache $msg"))
            }
        case Failure(t) =>
          Future(UpdateSchemaFailed(db, namespace, metric, List(t.getMessage)))
      }

  override def receive: Receive = {
    case GetSchema(db, namespace, metric) =>
      (schemaCache ? GetSchemaFromCache(db, namespace, metric))
        .map {
          case SchemaCached(_, _, _, schemaOpt) => SchemaGot(db, namespace, metric, schemaOpt)
          //TODO handle error
          case _ => SchemaGot(db, namespace, metric, None)
        }
        .pipeTo(sender)
    case UpdateSchemaFromRecord(db, namespace, metric, record) =>
      (schemaCache ? GetSchemaFromCache(db, namespace, metric))
        .flatMap {
          case SchemaCached(_, _, _, schemaOpt) =>
            (Schema(metric, record), schemaOpt) match {
              case (Success(newSchema), Some(oldSchema)) =>
                checkAndUpdateSchema(db, namespace, metric, oldSchema, newSchema)
              case (Success(newSchema), None) =>
                (schemaCache ? PutSchemaInCache(db, namespace, metric, newSchema))
                  .map {
                    case SchemaCached(_, _, _, _) =>
                      mediator ! Publish("schema", UpdateSchema(db, namespace, metric, newSchema))
                      SchemaUpdated(db, namespace, metric, newSchema)
                    case msg => UpdateSchemaFailed(db, namespace, metric, List(s"Unknown response from cache $msg"))
                  }
              case (Failure(t), _) => Future(UpdateSchemaFailed(db, namespace, metric, List(t.getMessage)))
            }

          //TODO handle error
          case _ => Future(SchemaGot(db, namespace, metric, None))
        }
        .pipeTo(sender)
    case DeleteSchema(db, namespace, metric) =>
      (schemaCache ? EvictSchema(db, namespace, metric))
        .map {
          case msg @ SchemaCached(`db`, `namespace`, `metric`, Some(_)) =>
            mediator ! Publish("schema", msg)
            SchemaDeleted(db, namespace, metric)
          case _ => SchemaDeleted(db, namespace, metric)
        }
        .pipeTo(sender)
    case DeleteNamespace(db, namespace) =>
      sender() ! NamespaceDeleted(db, namespace)
//      val schemaActorToDelete = getSchemaActor(db, namespace)
//      (schemaActorToDelete ? DeleteAllSchemas(db, namespace))
//        .map { _ =>
//          schemaActorToDelete ! PoisonPill
//          schemaActors -= NamespaceKey(db, namespace)
//          NamespaceDeleted(db, namespace)
//        }
//        .pipeTo(sender)
  }
}

object SchemaCoordinator {

  def props(basePath: String, schemaCache: ActorRef, mediator: ActorRef): Props =
    Props(new SchemaCoordinator(basePath, schemaCache, mediator))
}
