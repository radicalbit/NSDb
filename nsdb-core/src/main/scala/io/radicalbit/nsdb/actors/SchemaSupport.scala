package io.radicalbit.nsdb.actors

import java.nio.file.Paths

import akka.actor.Actor
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}
import org.apache.lucene.store.FSDirectory

import scala.collection.mutable

trait SchemaSupport { this: Actor =>

  def basePath: String

  def namespace: String

  lazy val schemaIndex = new SchemaIndex(FSDirectory.open(Paths.get(basePath, namespace, "schemas")))

  protected lazy val schemas: mutable.Map[String, Schema] = mutable.Map.empty

  override def preStart(): Unit = {
    schemas ++= schemaIndex.getAllSchemas.map(s => s.metric -> s).toMap
  }
}
