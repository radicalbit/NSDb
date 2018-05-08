/*
 * Copyright 2018 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.SchemaActor
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.collection.mutable

/**
  * Actor responsible for dispatching read and write schema operations to che proper schema actor.
  * @param basePath indexes' base path.
  */
class MetricsSchemaActor(val basePath: String) extends Actor with ActorLogging {

  val schemaActors: mutable.Map[NamespaceKey, ActorRef] = mutable.Map.empty

  private def getSchemaActor(db: String, namespace: String): ActorRef =
    schemaActors.getOrElse(
      NamespaceKey(db, namespace), {
        val schemaActor = context.actorOf(SchemaActor.props(basePath, db, namespace), s"schema-service-$db-$namespace")
        schemaActors += (NamespaceKey(db, namespace) -> schemaActor)
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
        .map { _ =>
          schemaActorToDelete ! PoisonPill
          schemaActors -= NamespaceKey(db, namespace)
          NamespaceDeleted(db, namespace)
        }
        .pipeTo(sender)
  }
}

object MetricsSchemaActor {

  def props(basePath: String): Props = Props(new MetricsSchemaActor(basePath))
}
