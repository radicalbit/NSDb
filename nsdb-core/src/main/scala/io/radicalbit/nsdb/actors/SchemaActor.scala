package io.radicalbit.nsdb.actors

import akka.actor.{Actor, ActorLogging, Props}
import cats.data.Validated.{Invalid, Valid}
import io.radicalbit.nsdb.actors.SchemaCoordinatorActor.commands._
import io.radicalbit.nsdb.actors.SchemaCoordinatorActor.events._
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}
import io.radicalbit.nsdb.model.SchemaField

class SchemaActor(val basePath: String, namespace: String)
    extends Actor
    with SchemaSupport
    with ActorLogging {

  override def receive: Receive = {

    case GetSchema(namespace, metric) =>
      val schema = getSchema(metric)
      sender ! SchemaGot(namespace, metric, schema)

    case UpdateSchema(namespace, metric, newSchema) =>
      checkAndUpdateSchema(namespace, metric, newSchema)

    case UpdateSchemaFromRecord(namespace, metric, record) =>
      (Schema(metric, record), getSchema(metric)) match {
        case (Valid(newSchema), Some(oldSchema)) =>
          checkAndUpdateSchema(namespace = namespace, metric = metric, oldSchema = oldSchema, newSchema = newSchema)
        case (Valid(newSchema), None) =>
          updateSchema(newSchema)
          sender ! SchemaUpdated(namespace, metric)
        case (Invalid(errs), _) => sender ! UpdateSchemaFailed(namespace, metric, errs.toList)
      }

    case DeleteSchema(namespace, metric) =>
      getSchema(metric) match {
        case Some(s) =>
          deleteSchema(s)
          sender ! SchemaDeleted(namespace, metric)
        case None => sender ! SchemaDeleted(namespace, metric)
      }
  }

  private def getSchema(metric: String) = schemas.get(metric) orElse schemaIndex.getSchema(metric)

  private def checkAndUpdateSchema(namespace: String, metric: String, newSchema: Schema): Unit =
    getSchema(metric) match {
      case Some(oldSchema) =>
        checkAndUpdateSchema(namespace = namespace, metric = metric, oldSchema = oldSchema, newSchema = newSchema)
      case None =>
        updateSchema(newSchema)
        sender ! SchemaUpdated(namespace, metric)
    }

  private def checkAndUpdateSchema(namespace: String, metric: String, oldSchema: Schema, newSchema: Schema): Unit =
    SchemaIndex.getCompatibleSchema(oldSchema, newSchema) match {
      case Valid(fields) =>
        updateSchema(metric, fields)
        sender ! SchemaUpdated(namespace, metric)
      case Invalid(list) => sender ! UpdateSchemaFailed(namespace, metric, list.toList)
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

  def props(basePath: String, namespace: String): Props = Props(new SchemaActor(basePath, namespace))

}
