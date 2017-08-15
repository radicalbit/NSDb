package io.radicalbit.nsdb.actors

import akka.actor.{Actor, Props}
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.nsdb.actors.SchemaActor.commands.{DeleteSchema, GetSchema, UpdateSchema, UpdateSchemaFromRecord}
import io.radicalbit.nsdb.actors.SchemaActor.events.{SchemaDeleted, SchemaGot, SchemaUpdated, UpdateSchemaFailed}
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}
import io.radicalbit.nsdb.model.Record

class SchemaActor(val basePath: String) extends Actor with SchemaSupport {

  private def getSchema(metric: String) = schemas.get(metric) orElse schemaIndex.getSchema(metric)

  override def receive: Receive = {
    case GetSchema(metric) =>
      val schema = getSchema(metric)
      sender ! SchemaGot(metric, schema)

    case UpdateSchema(metric, newSchema) =>
      getSchema(metric) match {
        case Some(oldSchema) =>
          SchemaIndex.getCompatibleSchema(oldSchema, newSchema) match {
            case Valid(fields) =>
              val updatedSchema = Schema(metric, fields)
              schemas += (metric -> updatedSchema)
              implicit val writer = schemaIndex.getWriter
              schemaIndex.update(metric, updatedSchema)
              writer.close()
              sender ! SchemaUpdated(metric)
            case Invalid(list) => sender ! UpdateSchemaFailed(metric, list.toList)
          }
        case None =>
          schemas += (metric -> newSchema)
          implicit val writer = schemaIndex.getWriter
          schemaIndex.update(metric, newSchema)
          writer.close()
          sender ! SchemaUpdated(metric)
      }
    case UpdateSchemaFromRecord(metric, record) =>
      (Schema(metric, record), getSchema(metric)) match {
        case (Valid(newSchema), Some(oldSchema)) =>
          SchemaIndex.getCompatibleSchema(oldSchema, newSchema) match {
            case Valid(fields) =>
              val updatedSchema = Schema(metric, fields)
              schemas += (metric -> updatedSchema)
              implicit val writer = schemaIndex.getWriter
              schemaIndex.update(metric, updatedSchema)
              writer.close()
              sender ! SchemaUpdated(metric)
            case Invalid(list) => sender ! UpdateSchemaFailed(metric, list.toList)
          }
        case (Valid(newSchema), None) =>
          schemas += (metric -> newSchema)
          implicit val writer = schemaIndex.getWriter
          schemaIndex.update(metric, newSchema)
          writer.close()
          sender ! SchemaUpdated(metric)
        case (Invalid(errs), _) => sender ! UpdateSchemaFailed(metric, errs.toList)
      }
    case DeleteSchema(metric) =>
      getSchema(metric) match {
        case Some(s) =>
          schemas -= metric
          implicit val writer = schemaIndex.getWriter
          schemaIndex.delete(s)
          writer.close()
          sender ! SchemaDeleted(metric)
        case None => sender ! SchemaDeleted(metric)
      }
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
