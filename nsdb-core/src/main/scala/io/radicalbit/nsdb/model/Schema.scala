/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.common.NSDbType
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}

import scala.util.{Failure, Success, Try}

/**
  * It models a bit field
  * @param name field name.
  * @param fieldClassType the field class Type [[FieldClassType]].
  * @param value field value.
  */
case class RawField(name: String, fieldClassType: FieldClassType, value: NSDbType)

/**
  * It models a bit field decorated by an internal type descriptor.
  * @param name field name.
  * @param fieldClassType the field class Type [[FieldClassType]].
  * @param indexType type descriptor.
  * @param value field value.
  */
case class TypedField(name: String, fieldClassType: FieldClassType, indexType: IndexType[_], value: NSDbType)

/**
  * It models a schema field.
  * @param name field name.
  * @param fieldClassType the field class Type [[FieldClassType]].
  * @param indexType the internal type descriptor.
  */
case class SchemaField(name: String, fieldClassType: FieldClassType, indexType: IndexType[_])

/**
  * Schema for metrics.
  * @param metric the metric.
  * @param fieldsMap a map of [[SchemaField]] keyed by field name
  */
class Schema private (val metric: String, val fieldsMap: Map[String, SchemaField]) extends NSDbSerializable {

  /**
    * filters tags from fieldsMap
    */
  lazy val tags: Map[String, SchemaField] = fieldsMap.filter { case (_, field) => field.fieldClassType == TagFieldType }

  /**
    * filters tags from fieldsMap
    */
  lazy val dimensions: Map[String, SchemaField] = fieldsMap.filter {
    case (_, field) => field.fieldClassType == DimensionFieldType
  }

  /**
    * True if there are no tags for the current schema.
    */
  lazy val isTagless: Boolean = tags.isEmpty

  /**
    * extract value from fieldsMap
    */
  lazy val value: SchemaField = fieldsMap("value")

  override def equals(obj: scala.Any): Boolean = {
    if (obj != null && obj.isInstanceOf[Schema]) {
      val otherSchema = obj.asInstanceOf[Schema]
      (otherSchema.metric == this.metric) && (otherSchema.fieldsMap.size == this.fieldsMap.size) && (otherSchema.fieldsMap == this.fieldsMap)
    } else false
  }
}

object Schema extends TypeSupport {

  /**
    * Creates a schema by analyzing a [[Bit]] structure.
    * @param metric the metric.
    * @param bit the bit to be used to create the schema.
    * @return the resulting [[Schema]]. If bit contains invalid fields the result will be a [[scala.util.Failure]]
    */
  def apply(metric: String, bit: Bit): Schema =
    validateSchemaTypeSupport(bit)
      .map(
        fieldsMap =>
          new Schema(
            metric,
            fieldsMap.mapValues(field => SchemaField(field.name, field.fieldClassType, field.indexType)).map(identity)))
      .get

  /**
    * Assemblies, if possible, the union schema from 2 given schemas.
    * Given 2 schemas, they are compatible if fields present in both of them are of the same types.
    * The union schema is a schema with the union of the dimension sets.
    * @param firstSchema the first schema.
    * @param secondSchema the second schema.
    * @return the union schema.
    */
  def union(firstSchema: Schema, secondSchema: Schema): Try[Schema] = {
    val notCompatibleFields = secondSchema.fieldsMap.collect {
      case (name, field)
          if firstSchema.fieldsMap.contains(name) && firstSchema.fieldsMap(field.name).indexType != field.indexType =>
        s"mismatch type for field ${field.name} : new type ${field.indexType} is incompatible with old type"
    }

    if (notCompatibleFields.nonEmpty)
      Failure(new RuntimeException(notCompatibleFields.mkString(",")))
    else {
      val schema = new Schema(secondSchema.metric, firstSchema.fieldsMap ++ secondSchema.fieldsMap)
      Success(schema)
    }
  }
}
