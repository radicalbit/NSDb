package io.radicalbit.nsdb.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.radicalbit.nsdb.actors.SchemaActor.commands._
import io.radicalbit.nsdb.index.Schema
import io.radicalbit.nsdb.model.Record

import scala.collection.mutable

class SchemaActor(val basePath: String) extends Actor with ActorLogging {

  val schemaActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  private def getSchemaActor(namespace: String): ActorRef =
    schemaActors.getOrElse(
      namespace, {
        val indexerActor = context.actorOf(IndexerActor.props(basePath, namespace), s"schema-service-$namespace")
        schemaActors += (namespace -> indexerActor)
        indexerActor
      }
    )

  override def receive = {
    case msg @ GetSchema(namespace, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ UpdateSchema(namespace, _, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ UpdateSchemaFromRecord(namespace, _, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ DeleteSchema(namespace, _) =>
      getSchemaActor(namespace).forward(msg)
  }
}

object SchemaActor {

  def props(basePath: String): Props = Props(new SchemaActor(basePath))

  object commands {
    case class GetSchema(namespace: String, metric: String)
    case class UpdateSchema(namespace: String, metric: String, newSchema: Schema)
    case class UpdateSchemaFromRecord(namespace: String, metric: String, record: Record)
    case class DeleteSchema(namespace: String, metric: String)
  }
  object events {
    case class SchemaGot(namespace: String, metric: String, schema: Option[Schema])
    case class SchemaUpdated(namespace: String, metric: String)
    case class UpdateSchemaFailed(namespace: String, metric: String, errors: List[String])
    case class SchemaDeleted(namespace: String, metric: String)
  }
}
