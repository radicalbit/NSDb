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

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props}
import io.radicalbit.nsdb.index.SchemaIndex
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.MMapDirectory

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  * Actor responsible to manage schemas for a given db and namespce.
  * For the sake of a highest throughput, schemas are stored in a in-memory structure and periodically stored into a lucene index.
  * It is possible to execute CRUD operations on schemas:
  *
  * - [[GetSchema]] retrieve a schema.
  *
  * - [[UpdateSchemaFromRecord]] update an existing schema given a new record. It the new record is invalid, the operation will be rejected.
  *
  * - [[DeleteSchema]] delete an existing schema.
  *
  * - [[DeleteAllSchemas]] delete all the schemas for the given db and namespace.
  *
  * @param basePath index base path.
  * @param db the db.
  * @param namespace the namespace.
  */
class SchemaActor(val basePath: String, val db: String, val namespace: String) extends Actor with ActorLogging {

  /**
    * the lucene index to store schemas in
    */
  lazy val schemaIndex = new SchemaIndex(new MMapDirectory(Paths.get(basePath, db, namespace, "schemas")))

  /**
    * mutable map containing schemas used to retrieve and store them
    */
  lazy val schemas: mutable.Map[String, Schema] = mutable.Map.empty

  /**
    * Buffer collecting write operations on the index.
    */
  lazy val schemasToWrite: mutable.ListBuffer[Schema] = mutable.ListBuffer.empty

  lazy val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  override def preStart(): Unit = {

    /**
      * before to start the actor, its state is initialized by retrieving schemas from the index.
      */
    schemaIndex.all.foreach(s => schemas += (s.metric -> s))

    /**
      * schedules write to index operations.
      */
    context.system.scheduler.schedule(interval, interval) {
      if (schemasToWrite.nonEmpty) {
        updateSchemaIndex(schemasToWrite)
        schemasToWrite.clear
      }
    }
  }

  override def receive: Receive = {

    case GetSchema(_, _, metric) =>
      val schema = getCachedSchema(metric)
      sender ! SchemaGot(db, namespace, metric, schema)

    case UpdateSchemaFromRecord(_, _, metric, record) =>
      (Schema(metric, record), getCachedSchema(metric)) match {
        case (Success(newSchema), Some(oldSchema)) =>
          checkAndUpdateSchema(namespace = namespace, metric = metric, oldSchema = oldSchema, newSchema = newSchema)
        case (Success(newSchema), None) =>
          sender ! SchemaUpdated(db, namespace, metric, newSchema)
          schemas += (metric -> newSchema)
          schemasToWrite += newSchema
        case (Failure(t), _) => sender ! UpdateSchemaFailed(db, namespace, metric, List(t.getMessage))
      }

    case DeleteSchema(_, _, metric) =>
      getCachedSchema(metric) match {
        case Some(s) =>
          deleteSchema(s)
          sender ! SchemaDeleted(db, namespace, metric)
        case None => sender ! SchemaDeleted(db, namespace, metric)
      }

    case DeleteAllSchemas(_, _) =>
      deleteAllSchemas()
      sender ! AllSchemasDeleted(db, namespace)
  }

  /**
    * Gets a schema from memory.
    * @param metric schema's metric
    * @return the schema if exist, None otherwise.
    */
  private def getCachedSchema(metric: String) = schemas.get(metric)

  /**
    * Checks if a newSchema is compatible with an oldSchema. If schemas are compatible, the metric schema will be updated.
    * @param namespace schema's namespace.
    * @param metric schema's metric.
    * @param oldSchema current schema for metric
    * @param newSchema schema to be checked and updated.
    */
  private def checkAndUpdateSchema(namespace: String, metric: String, oldSchema: Schema, newSchema: Schema): Unit =
    if (oldSchema == newSchema)
      sender ! SchemaUpdated(db, namespace, metric, newSchema)
    else
      SchemaIndex.union(oldSchema, newSchema) match {
        case Success(schema) =>
          sender ! SchemaUpdated(db, namespace, metric, newSchema)
          schemas += (schema.metric -> schema)
          schemasToWrite += schema
        case Failure(t) => sender ! UpdateSchemaFailed(db, namespace, metric, List(t.getMessage))
      }

  /**
    * Updates schema index.
    * @param schemas schemas to be updated.
    */
  private def updateSchemaIndex(schemas: Seq[Schema]): Unit = {
    implicit val writer: IndexWriter = schemaIndex.getWriter
    schemas.foreach(schema => {
      schemaIndex.update(schema.metric, schema)
    })
    writer.close()
    schemaIndex.refresh()
  }

  /**
    * Deletes a schema from memory and from index.
    * @param schema the schema to be deleted.
    */
  private def deleteSchema(schema: Schema): Unit = {
    schemas -= schema.metric
    implicit val writer: IndexWriter = schemaIndex.getWriter
    schemaIndex.delete(schema)
    writer.close()
    schemaIndex.refresh()
  }

  /**
    * Deletes all schemas for the db and the namespace provided in actor initialization.
    */
  private def deleteAllSchemas(): Unit = {
    schemas --= schemas.keys
    implicit val writer: IndexWriter = schemaIndex.getWriter
    schemaIndex.deleteAll()
    writer.close()
    schemaIndex.refresh()
  }
}

object SchemaActor {

  def props(basePath: String, db: String, namespace: String): Props = Props(new SchemaActor(basePath, db, namespace))

}
