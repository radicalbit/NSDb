package io.radicalbit.nsdb.actors

import java.nio.file.Paths

import akka.actor.Actor
import io.radicalbit.nsdb.index.{Schema, SchemaIndex}
import org.apache.lucene.index.IndexNotFoundException
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.MMapDirectory

import scala.collection.mutable

trait SchemaSupport { this: Actor =>

  def basePath: String

  def db: String

  def namespace: String

  lazy val schemaIndex = new SchemaIndex(new MMapDirectory(Paths.get(basePath, db, namespace, "schemas")))

  lazy val schemas: mutable.Map[String, Schema] = mutable.Map.empty

  override def preStart(): Unit = {
    try {
      implicit val searcher: IndexSearcher = schemaIndex.getSearcher
      schemas ++= schemaIndex.getAllSchemas.map(s => s.metric -> s).toMap
    } catch {
      case _: IndexNotFoundException => //do nothing
    }
  }
}
