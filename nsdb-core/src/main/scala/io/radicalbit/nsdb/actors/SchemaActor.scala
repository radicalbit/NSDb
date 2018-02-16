package io.radicalbit.nsdb.actors

import akka.actor.{Actor, ActorLogging, Props}
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.apache.lucene.index.IndexWriter

import scala.util.{Failure, Success}

class SchemaActor(val basePath: String, val db: String, val namespace: String)
    extends Actor
    with SchemaSupport
    with ActorLogging {

  override def receive: Receive = {

    case GetSchema(_, _, metric) =>
      val schema = getSchema(metric)
      sender ! SchemaGot(db, namespace, metric, schema)

    case UpdateSchemaFromRecord(_, _, metric, record) =>
      (Schema(metric, record), getSchema(metric)) match {
        case (Success(newSchema), Some(oldSchema)) =>
          checkAndUpdateSchema(namespace = namespace, metric = metric, oldSchema = oldSchema, newSchema = newSchema)
        case (Success(newSchema), None) =>
          sender ! SchemaUpdated(db, namespace, metric, newSchema)
          updateSchema(newSchema)
        case (Failure(t), _) => sender ! UpdateSchemaFailed(db, namespace, metric, List(t.getMessage))
      }

    case DeleteSchema(_, _, metric) =>
      getSchema(metric) match {
        case Some(s) =>
          deleteSchema(s)
          sender ! SchemaDeleted(db, namespace, metric)
        case None => sender ! SchemaDeleted(db, namespace, metric)
      }

    case DeleteAllSchemas(_, _) =>
      deleteAllSchemas()
      sender ! AllSchemasDeleted(db, namespace)
  }

  private def getSchema(metric: String) = schemas.get(metric) orElse schemaIndex.getSchema(metric)

  private def checkAndUpdateSchema(namespace: String, metric: String, oldSchema: Schema, newSchema: Schema): Unit =
    if (oldSchema == newSchema)
      sender ! SchemaUpdated(db, namespace, metric, newSchema)
    else
      SchemaIndex.getCompatibleSchema(oldSchema, newSchema) match {
        case Success(fields) =>
          sender ! SchemaUpdated(db, namespace, metric, newSchema)
          updateSchema(metric, fields)
        case Failure(t) => sender ! UpdateSchemaFailed(db, namespace, metric, List(t.getMessage))
      }

  private def updateSchema(metric: String, fields: Seq[SchemaField]): Unit =
    updateSchema(Schema(metric, fields))

  private def updateSchema(schema: Schema): Unit = {
    schemas += (schema.metric -> schema)
    implicit val writer: IndexWriter = schemaIndex.getWriter
    schemaIndex.update(schema.metric, schema)
    writer.close()
    schemaIndex.refresh()
  }

  private def deleteSchema(schema: Schema): Unit = {
    schemas -= schema.metric
    implicit val writer: IndexWriter = schemaIndex.getWriter
    schemaIndex.delete(schema)
    writer.close()
  }

  private def deleteAllSchemas(): Unit = {
    schemas --= schemas.keys
    implicit val writer: IndexWriter = schemaIndex.getWriter
    schemaIndex.deleteAll()
    writer.close()
  }
}

object SchemaActor {

  def props(basePath: String, db: String, namespace: String): Props = Props(new SchemaActor(basePath, db, namespace))

}
