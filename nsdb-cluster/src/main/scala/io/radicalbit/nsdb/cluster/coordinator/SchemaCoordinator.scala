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

package io.radicalbit.nsdb.cluster.coordinator

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props, Stash}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.commands.DeleteNamespaceSchema
import io.radicalbit.nsdb.cluster.coordinator.SchemaCoordinator.events.{
  NamespaceSchemaDeleted,
  SchemaMigrated,
  SchemaMigrationFailed
}
import io.radicalbit.nsdb.cluster.util.FileUtils
import io.radicalbit.nsdb.common.protocol.{Coordinates, NSDbSerializable}
import io.radicalbit.nsdb.index.{DirectorySupport, SchemaIndex}
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.util.ActorPathLogging
import org.apache.lucene.index.IndexUpgrader

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Actor responsible for dispatching read and write schema operations to che proper schema actor.
  * It performs write/update/deletion in distributed cache
  *
  */
class SchemaCoordinator(schemaCache: ActorRef) extends ActorPathLogging with Stash with DirectorySupport {

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
                                   newSchema: Schema): Future[SchemaUpdateResponse] =
    if (oldSchema == newSchema) {
      Future(SchemaUpdated(db, namespace, metric, newSchema))
    } else
      Schema.union(oldSchema, newSchema) match {
        case Success(unionSchema) =>
          (schemaCache ? PutSchemaInCache(db, namespace, metric, unionSchema))
            .map {
              case SchemaCached(_, _, _, _) =>
                SchemaUpdated(db, namespace, metric, unionSchema)
              case msg =>
                UpdateSchemaFailed(db, namespace, metric, List(s"Unknown response from schema cache $msg"))
            }
        case Failure(t) =>
          Future(UpdateSchemaFailed(db, namespace, metric, List(t.getMessage)))
      }

  override def receive: Receive = {
    case GetSchema(db, namespace, metric) =>
      (schemaCache ? GetSchemaFromCache(db, namespace, metric))
        .map {
          case SchemaCached(_, _, _, schemaOpt) => SchemaGot(db, namespace, metric, schemaOpt)

          case e =>
            log.error(s"unexpected response from cache: expecting SchemaCached while got {}", e)
            GetSchemaFailed(db,
                            namespace,
                            metric,
                            s"unexpected response from cache: expecting SchemaCached while got $e")
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
                      SchemaUpdated(db, namespace, metric, newSchema)
                    case msg => UpdateSchemaFailed(db, namespace, metric, List(s"Unknown response from cache $msg"))
                  }
              case (Failure(t), _) =>
                Future(UpdateSchemaFailed(db, namespace, metric, List(t.getMessage)))
            }
          case e =>
            log.error("unexpected response from cache: expecting SchemaCached while got {}", e)
            Future(
              GetSchemaFailed(db,
                              namespace,
                              metric,
                              s"unexpected response from cache: expecting SchemaCached while got $e"))
        } pipeTo sender()
    case DeleteSchema(db, namespace, metric) =>
      (schemaCache ? EvictSchema(db, namespace, metric))
        .map {
          case SchemaCached(`db`, `namespace`, `metric`, _) =>
            SchemaDeleted(db, namespace, metric)
          case _ =>
            SchemaDeleted(db, namespace, metric)
        }
        .pipeTo(sender)
    case DeleteNamespace(db, namespace) =>
      (schemaCache ? DeleteNamespaceSchema(db, namespace))
        .map {
          case NamespaceSchemaDeleted(_, _) =>
            NamespaceDeleted(db, namespace)
          //FIXME:  always positive response
          case _ => NamespaceDeleted(db, namespace)
        }
        .pipeTo(sender())
    case Migrate(inputPath) =>
      log.info("migrating schemas for {}", inputPath)
      val allSchemas = FileUtils.getSubDirs(inputPath).flatMap { db =>
        FileUtils.getSubDirs(db).map { namespace =>
          val schemaIndexDir = createMmapDirectory(Paths.get(inputPath, db.getName, namespace.getName, "schemas"))
          new IndexUpgrader(schemaIndexDir).upgrade()
          val schemaIndex = new SchemaIndex(schemaIndexDir)
          val schemas     = schemaIndex.all
          schemaIndex.close()
          (db.getName, namespace.getName, schemas)
        }
      }

      import cats.instances.either._
      import cats.instances.list._
      import cats.syntax.traverse._

      Future
        .sequence(allSchemas.map {
          case (db, namespace, schemas) =>
            Future.sequence(schemas.map {
              schema =>
                (schemaCache ? GetSchemaFromCache(db, namespace, schema.metric))
                  .flatMap {
                    case SchemaCached(_, _, _, Some(oldSchema)) =>
                      checkAndUpdateSchema(db, namespace, schema.metric, oldSchema, schema).map {
                        case msg: SchemaUpdated      => Right(msg)
                        case msg: UpdateSchemaFailed => Left(msg)
                      }
                    case SchemaCached(_, _, _, None) =>
                      (schemaCache ? PutSchemaInCache(db, namespace, schema.metric, schema))
                        .map {
                          case SchemaCached(_, _, _, _) =>
                            Right(SchemaUpdated(db, namespace, schema.metric, schema))
                          case msg =>
                            Left(
                              UpdateSchemaFailed(db,
                                                 namespace,
                                                 schema.metric,
                                                 List(s"Unknown response from cache $msg")))
                        }
                    case _ =>
                      Future(
                        Left(UpdateSchemaFailed(db, namespace, schema.metric, List(s"Unknown response from cache"))))
                  }
            })
        })
        .map(_.flatten.sequence)
        .map {
          case Right(seq) => SchemaMigrated(seq.map(e => (Coordinates(e.db, e.namespace, e.metric), e.schema)))
          case Left(UpdateSchemaFailed(db: String, namespace: String, metric: String, errors: List[String])) =>
            SchemaMigrationFailed(db, namespace, metric, errors)
        }
        .pipeTo(sender())

  }
}

object SchemaCoordinator {

  def props(schemaCache: ActorRef): Props =
    Props(new SchemaCoordinator(schemaCache))

  object events {
    case class NamespaceSchemaDeleted(db: String, namespace: String) extends NSDbSerializable
    case class SchemaMigrated(schemas: Seq[(Coordinates, Schema)])   extends NSDbSerializable
    case class SchemaMigrationFailed(db: String, namespace: String, metric: String, errors: List[String])
        extends NSDbSerializable
  }

  object commands {
    case class DeleteNamespaceSchema(db: String, namespace: String) extends NSDbSerializable
  }
}
