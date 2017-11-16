package io.radicalbit.nsdb.actors

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.collection.mutable

class NamespaceSchemaActor(val basePath: String) extends Actor with ActorLogging {

  val schemaActors: mutable.Map[String, ActorRef] = mutable.Map.empty

  private def getSchemaActor(namespace: String): ActorRef =
    schemaActors.getOrElse(
      namespace, {
        val schemaActor = context.actorOf(SchemaActor.props(basePath, namespace), s"schema-service-$namespace")
        schemaActors += (namespace -> schemaActor)
        schemaActor
      }
    )

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.namespace-schema.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  override def receive: Receive = {
    case msg @ GetSchema(namespace, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ UpdateSchema(namespace, _, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ UpdateSchemaFromRecord(namespace, _, _) =>
      getSchemaActor(namespace).forward(msg)
    case msg @ DeleteSchema(namespace, _) =>
      getSchemaActor(namespace).forward(msg)
    case DeleteNamespace(namespace) =>
      val schemaActorToDelete = getSchemaActor(namespace)
      (schemaActorToDelete ? DeleteAllSchemas(namespace))
        .mapTo[AllSchemasDeleted]
        .map { _ =>
          schemaActorToDelete ! PoisonPill
          schemaActors -= namespace
          NamespaceDeleted(namespace)
        }
        .pipeTo(sender)
  }
}

object NamespaceSchemaActor {

  def props(basePath: String): Props = Props(new NamespaceSchemaActor(basePath))
}
