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

package io.radicalbit.nsdb.connector.kafka.sink

import com.datamountaineer.kcql.Kcql
import scala.collection.JavaConverters._

/**
  * Parsed information from a kcql expression
  * @param dbField nsdb db field to be fetched from topic data.
  * @param namespaceField nsdb namespace field to be fetched from topic data.
  * @param metric nsdb metric.
  * @param aliasesMap nsdb aliases map (e.g. `alias -> field` means that `field` must be fetched from topic and saved as `alias` to nsdb
  */
case class ParsedKcql(dbField: String, namespaceField: String, metric: String, aliasesMap: Map[String, String])

object ParsedKcql {

  /**
    * Returns an instance of [[ParsedKcql]] from a kcql string.
    * @param queryString the string to be parsed.
    * @param globalDb the db defined as a config param if present.
    * @param globalNamespace the namespace defined as a config param if present.
    * @return the instance of [[ParsedKcql]].
    * @throws IllegalArgumentException if queryString is not valid.
    */
  def apply(queryString: String, globalDb: Option[String], globalNamespace: Option[String]): ParsedKcql = {
    this(Kcql.parse(queryString), globalDb, globalNamespace)
  }

  /**
    * Returns an instance of [[ParsedKcql]] from a [[Kcql]].
    * @param kcql the kcql to be parsed.
    * @param globalDb the db defined as a config param if present.
    * @param globalNamespace the namespace defined as a config param if present.
    * @return the instance of [[ParsedKcql]].
    * @throws IllegalArgumentException if input kcql is not valid.
    */
  def apply(kcql: Kcql, globalDb: Option[String], globalNamespace: Option[String]): ParsedKcql = {

    val aliasesMap = kcql.getFields.asScala.map(f => f.getAlias -> f.getName).toMap

    val db        = aliasesMap.get("db") orElse globalDb
    val namespace = aliasesMap.get("namespace") orElse globalNamespace
    val metric    = kcql.getTarget

    require(db.isDefined, "A global db configuration or a Db alias in Kcql must be defined")
    require(namespace.isDefined, "A global namespace configuration or a Namespace alias in Kcql must be defined")
    require(kcql.getTimestamp != null && kcql.getTimestamp.nonEmpty)
    require(aliasesMap.get("value").isDefined, "Value alias in kcql must be defined")

    ParsedKcql(db.get, namespace.get, metric, aliasesMap - "db" - "namespace" + ("timestamp" -> kcql.getTimestamp))
  }
}
