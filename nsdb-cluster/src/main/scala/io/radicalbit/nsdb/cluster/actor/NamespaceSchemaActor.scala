package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.SchemaActor
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.collection.mutable

class NamespaceSchemaActor(val basePath: String) extends Actor with ActorLogging {

  val schemaActors: mutable.Map[(String, String), ActorRef] = mutable.Map.empty

  private def getSchemaActor(db: String, namespace: String): ActorRef =
    schemaActors.getOrElse(
      (db, namespace), {
        val schemaActor = context.actorOf(SchemaActor.props(basePath, db, namespace), s"schema-service-$db-$namespace")
        schemaActors += ((db, namespace) -> schemaActor)
        schemaActor
      }
    )

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.namespace-schema.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  override def receive: Receive = {
    case msg @ GetSchema(db, namespace, _) =>
      getSchemaActor(db, namespace).forward(msg)
    case msg @ UpdateSchemaFromRecord(db, namespace, _, _) =>
      getSchemaActor(db, namespace).forward(msg)
    case msg @ DeleteSchema(db, namespace, _) =>
      getSchemaActor(db, namespace).forward(msg)
    case DeleteNamespace(db, namespace) =>
      val schemaActorToDelete = getSchemaActor(db, namespace)
      (schemaActorToDelete ? DeleteAllSchemas(db, namespace))
        .mapTo[AllSchemasDeleted]
        .map { _ =>
          schemaActorToDelete ! PoisonPill
          schemaActors -= ((db, namespace))
          NamespaceDeleted(db, namespace)
        }
        .pipeTo(sender)
  }
}

object NamespaceSchemaActor {

  def props(basePath: String): Props = Props(new NamespaceSchemaActor(basePath))
}
