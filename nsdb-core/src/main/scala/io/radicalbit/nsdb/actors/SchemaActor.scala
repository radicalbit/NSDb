package io.radicalbit.nsdb.actors

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props}
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.MMapDirectory

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class SchemaActor(val basePath: String, val db: String, val namespace: String) extends Actor with ActorLogging {

  lazy val schemaIndex = new SchemaIndex(new MMapDirectory(Paths.get(basePath, db, namespace, "schemas")))

  lazy val schemas: mutable.Map[String, Schema] = mutable.Map.empty

  lazy val schemasToWrite: mutable.ListBuffer[Schema] = mutable.ListBuffer.empty

  lazy val interval = FiniteDuration(
    context.system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  override def preStart(): Unit = {
    schemaIndex.allSchemas.foreach(s => schemas += (s.metric -> s))

    context.system.scheduler.schedule(interval, interval) {
      if (schemasToWrite.nonEmpty) {
        updateSchemas(schemasToWrite)
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

  private def getCachedSchema(metric: String) = schemas.get(metric)

  private def checkAndUpdateSchema(namespace: String, metric: String, oldSchema: Schema, newSchema: Schema): Unit =
    if (oldSchema == newSchema)
      sender ! SchemaUpdated(db, namespace, metric, newSchema)
    else
      SchemaIndex.getCompatibleSchema(oldSchema, newSchema) match {
        case Success(schema) =>
          sender ! SchemaUpdated(db, namespace, metric, newSchema)
          schemas += (schema.metric -> schema)
          schemasToWrite += schema
        case Failure(t) => sender ! UpdateSchemaFailed(db, namespace, metric, List(t.getMessage))
      }

  private def updateSchemas(schemas: Seq[Schema]): Unit = {
    implicit val writer: IndexWriter = schemaIndex.getWriter
    schemas.foreach(schema => {
      schemaIndex.update(schema.metric, schema)
    })
    writer.close()
    schemaIndex.refresh()
  }

  private def deleteSchema(schema: Schema): Unit = {
    schemas -= schema.metric
    implicit val writer: IndexWriter = schemaIndex.getWriter
    schemaIndex.delete(schema)
    writer.close()
    schemaIndex.refresh()
  }

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
