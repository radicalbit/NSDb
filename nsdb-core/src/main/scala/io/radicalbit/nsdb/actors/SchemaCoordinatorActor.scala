package io.radicalbit.nsdb.actors

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.SchemaCoordinatorActor.commands._
import io.radicalbit.nsdb.actors.SchemaCoordinatorActor.events.AllSchemasDeleted
import io.radicalbit.nsdb.index.Schema
import io.radicalbit.nsdb.model.Record

import scala.collection.mutable
import scala.concurrent.duration._

class SchemaCoordinatorActor(val basePath: String) extends Actor with ActorLogging {

  val schemaActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  private def getSchemaActor(namespace: String): ActorRef =
    schemaActors.getOrElse(
      namespace, {
        val indexerActor = context.actorOf(IndexerActor.props(basePath, namespace), s"schema-service-$namespace")
        schemaActors += (namespace -> indexerActor)
        indexerActor
      }
    )

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  override def receive = {
    case msg @ GetSchema(namespace, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ UpdateSchema(namespace, _, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ UpdateSchemaFromRecord(namespace, _, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ DeleteSchema(namespace, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ DeleteAllSchemas(namespace) =>
      val schemaActorToDelete = getSchemaActor(namespace)
      (schemaActorToDelete ? msg)
        .map { e =>
          schemaActorToDelete ! PoisonPill
          schemaActors -= namespace
          AllSchemasDeleted(namespace)
        }
        .pipeTo(sender)
  }
}

object SchemaCoordinatorActor {

  def props(basePath: String): Props = Props(new SchemaCoordinatorActor(basePath))

  object commands {
    case class GetSchema(namespace: String, metric: String)
    case class UpdateSchema(namespace: String, metric: String, newSchema: Schema)
    case class UpdateSchemaFromRecord(namespace: String, metric: String, record: Record)
    case class DeleteSchema(namespace: String, metric: String)
    case class DeleteAllSchemas(namespace: String)
  }
  object events {
    case class SchemaGot(namespace: String, metric: String, schema: Option[Schema])
    case class SchemaUpdated(namespace: String, metric: String)
    case class UpdateSchemaFailed(namespace: String, metric: String, errors: List[String])
    case class SchemaDeleted(namespace: String, metric: String)
    case class AllSchemasDeleted(namespace: String)
  }
}
