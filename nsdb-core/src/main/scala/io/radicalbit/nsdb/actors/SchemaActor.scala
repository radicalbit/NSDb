package io.radicalbit.nsdb.actors

import akka.actor.{Actor, Props}
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.nsdb.actors.SchemaActor.commands.{GetSchema, UpdateSchema}
import io.radicalbit.nsdb.actors.SchemaActor.events.{SchemaGot, SchemaUpdated, UpdateSchemaFailed}
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}

class SchemaActor(val basePath: String) extends Actor with SchemaSupport {

  private def getSchema(metric: String) = schemas.get(metric) orElse schemaIndex.getSchema(metric)

  override def receive = {
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
  }
}

object SchemaActor {

  def props(basePath: String): Props = Props(new SchemaActor(basePath))

  object commands {
    case class GetSchema(metric: String)
    case class UpdateSchema(metric: String, newSchema: Schema)
  }
  object events {
    case class SchemaGot(metric: String, schema: Option[Schema])
    case class SchemaUpdated(metric: String)
    case class UpdateSchemaFailed(metric: String, errors: List[String])
  }
}
