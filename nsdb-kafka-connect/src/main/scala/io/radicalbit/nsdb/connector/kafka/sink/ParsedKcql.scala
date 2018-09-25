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
import io.radicalbit.nsdb.connector.kafka.sink.conf.Constants._

import scala.collection.JavaConverters._

/**
  * Parsed information from a kcql expression.
  * For all the aliases maps `alias -> field` means that `field` must be fetched from topic and saved as `alias` to NSDb.
  * @param dbField NSDb db field to be fetched from topic data.
  * @param namespaceField NSDb namespace field to be fetched from topic data.
  * @param metric NSDb metric.
  * @param timestampField timestamp field if present (current timestamp is used otherwise).
  * @param valueField value field if present (default value is used otherwise).
  * @param tagAliasesMap NSDb tags aliases map.
  * @param dimensionAliasesMap NSDb dimensions aliases map.
  */
case class ParsedKcql(dbField: String,
                      namespaceField: String,
                      metric: String,
                      defaultValue: Option[java.math.BigDecimal],
                      timestampField: Option[String],
                      valueField: Option[String],
                      tagAliasesMap: Map[String, String],
                      dimensionAliasesMap: Map[String, String])

object ParsedKcql {

  /**
    * Returns an instance of [[ParsedKcql]] from a kcql string.
    * @param queryString the string to be parsed.
    * @param globalDb the db defined as a config param if present.
    * @param globalNamespace the namespace defined as a config param if present.
    * @param defaultValue the default value defined as a config param if present.
    * @return the instance of [[ParsedKcql]].
    * @throws IllegalArgumentException if queryString is not valid.
    */
  def apply(queryString: String,
            globalDb: Option[String],
            globalNamespace: Option[String],
            defaultValue: Option[java.math.BigDecimal]): ParsedKcql = {
    this(Kcql.parse(queryString), globalDb, globalNamespace, defaultValue)
  }

  /**
    * Returns an instance of [[ParsedKcql]] from a [[Kcql]].
    * @param kcql the kcql to be parsed.
    * @param globalDb the db defined as a config param if present. In values map, will be used both for key and value.
    * @param globalNamespace the namespace defined as a config param if present.
    *                        In values map, will be used both for key and value.
    * @param defaultValue the default value defined as a config param if present.
    * @return the instance of [[ParsedKcql]].
    * @throws IllegalArgumentException if input kcql is not valid.
    */
  def apply(kcql: Kcql,
            globalDb: Option[String],
            globalNamespace: Option[String],
            defaultValue: Option[java.math.BigDecimal]): ParsedKcql = {

    val aliasesMap = kcql.getFields.asScala.map(f => f.getAlias -> f.getName).toMap

    val db        = aliasesMap.get(Parsed.DbFieldName) orElse globalDb
    val namespace = aliasesMap.get(Parsed.NamespaceFieldName) orElse globalNamespace
    val metric    = kcql.getTarget

    val tagAliases = Option(kcql.getTags)
      .map(_.asScala)
      .getOrElse(List.empty)
      .map(e => e.getKey -> Option(e.getValue).getOrElse(e.getKey))
      .toMap

    require(db.isDefined, "A global db configuration or a Db alias in Kcql must be defined")
    require(namespace.isDefined, "A global namespace configuration or a Namespace alias in Kcql must be defined")
    require(
      aliasesMap.get(Writer.ValueFieldName).isDefined || defaultValue.isDefined,
      "Value alias in kcql must be defined"
    )

    val allAliases = aliasesMap - Parsed.DbFieldName - Parsed.NamespaceFieldName - Writer.ValueFieldName

    val (tags: Map[String, String], dimensions: Map[String, String]) = allAliases.partition {
      case (k, v) => tagAliases.get(k).isDefined
    }

    ParsedKcql(
      db.get,
      namespace.get,
      metric,
      defaultValue,
      Option(kcql.getTimestamp),
      aliasesMap.get(Writer.ValueFieldName),
      tags,
      dimensions
    )
  }
}
