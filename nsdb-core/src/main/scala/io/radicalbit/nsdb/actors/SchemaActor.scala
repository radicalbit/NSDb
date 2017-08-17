package io.radicalbit.nsdb.actors

import akka.actor.{Actor, ActorLogging, Props}
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.nsdb.actors.SchemaActor.commands._
import io.radicalbit.nsdb.actors.SchemaActor.events._
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}
import io.radicalbit.nsdb.model.{Record, SchemaField}

class SchemaActor(val basePath: String) extends Actor with SchemaSupport with ActorLogging {

  override def receive: Receive = {

    case GetSchema(metric) =>
      val schema = getSchema(metric)
      sender ! SchemaGot(metric, schema)

    case UpdateSchema(metric, newSchema) =>
      checkAndUpdateSchema(metric, newSchema)

    case UpdateSchemaFromRecord(metric, record) =>
      (Schema(metric, record), getSchema(metric)) match {
        case (Valid(newSchema), Some(oldSchema)) =>
          checkAndUpdateSchema(metric = metric, oldSchema = oldSchema, newSchema = newSchema)
        case (Valid(newSchema), None) =>
          updateSchema(newSchema)
          sender ! SchemaUpdated(metric)
        case (Invalid(errs), _) => sender ! UpdateSchemaFailed(metric, errs.toList)
      }

    case DeleteSchema(metric) =>
      getSchema(metric) match {
        case Some(s) =>
          deleteSchema(s)
          sender ! SchemaDeleted(metric)
        case None => sender ! SchemaDeleted(metric)
      }
  }

  private def getSchema(metric: String) = schemas.get(metric) orElse schemaIndex.getSchema(metric)

  private def checkAndUpdateSchema(metric: String, newSchema: Schema): Unit =
    getSchema(metric) match {
      case Some(oldSchema) =>
        checkAndUpdateSchema(metric = metric, oldSchema = oldSchema, newSchema = newSchema)
      case None =>
        updateSchema(newSchema)
        sender ! SchemaUpdated(metric)
    }

  private def checkAndUpdateSchema(metric: String, oldSchema: Schema, newSchema: Schema): Unit =
    SchemaIndex.getCompatibleSchema(oldSchema, newSchema) match {
    case Valid(fields) =>
      updateSchema(metric, fields)
      sender ! SchemaUpdated(metric)
    case Invalid(list) => sender ! UpdateSchemaFailed(metric, list.toList)
  }

  private def updateSchema(metric: String, fields: Seq[SchemaField]): Unit =
    updateSchema(Schema(metric, fields))

  private def updateSchema(schema: Schema): Unit = {
    schemas += (schema.metric -> schema)
    implicit val writer = schemaIndex.getWriter
    schemaIndex.update(schema.metric, schema)
    writer.close()
  }

  private def deleteSchema(schema: Schema): Unit = {
    schemas -= schema.metric
    implicit val writer = schemaIndex.getWriter
    schemaIndex.delete(schema)
    writer.close()
  }
}

object SchemaActor {

  def props(basePath: String): Props = Props(new SchemaActor(basePath))

  object commands {
    case class GetSchema(metric: String)
    case class UpdateSchema(metric: String, newSchema: Schema)
    case class UpdateSchemaFromRecord(metric: String, record: Record)
    case class DeleteSchema(metric: String)
  }
  object events {
    case class SchemaGot(metric: String, schema: Option[Schema])
    case class SchemaUpdated(metric: String)
    case class UpdateSchemaFailed(metric: String, errors: List[String])
    case class SchemaDeleted(metric: String)
  }
}
