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

package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.{Bit, FieldClassType}
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}

import scala.util.{Failure, Success, Try}

/**
  * It models a bit field
  * @param name field name.
  * @param fieldClassType the field class Type [[FieldClassType]].
  * @param value field value.
  */
case class RawField(name: String, fieldClassType: FieldClassType, value: JSerializable)

/**
  * It models a bit field decorated by an internal type descriptor.
  * @param name field name.
  * @param fieldClassType the field class Type [[FieldClassType]].
  * @param indexType type descriptor.
  * @param value field value.
  */
case class TypedField(name: String, fieldClassType: FieldClassType, indexType: IndexType[_], value: JSerializable)

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
  * @param fields a set of [[SchemaField]]
  */
case class Schema(metric: String, fields: Set[SchemaField]) {
  override def equals(obj: scala.Any): Boolean = {
    if (obj != null && obj.isInstanceOf[Schema]) {
      val otherSchema = obj.asInstanceOf[Schema]
      (otherSchema.metric == this.metric) && (otherSchema.fields.size == this.fields.size) && (otherSchema.fields == this.fields)
    } else false
  }

  def fieldsMap: Map[String, SchemaField] =
    fields.map(f => f.name -> f).toMap
}

object Schema extends TypeSupport {

  /**
    * Creates a schema by analyzing a [[Bit]] structure.
    * @param metric the metric.
    * @param bit the bit to be used to create the schema.
    * @return the resulting [[Schema]]. If bit contains invalid fields the result will be a [[scala.util.Failure]]
    */
  def apply(metric: String, bit: Bit): Try[Schema] = {
    validateSchemaTypeSupport(bit).map((fields: Seq[TypedField]) =>
      Schema(metric, fields.map(field => SchemaField(field.name, field.fieldClassType, field.indexType)).toSet))
  }

  /**
    * Assemblies, if possible, the union schema from 2 given schemas.
    * Given 2 schemas, they are compatible if fields present in both of them are of the same types.
    * The union schema is a schema with the union of the dimension sets.
    * @param firstSchema the first schema.
    * @param secondSchema the second schema.
    * @return the union schema.
    */
  def union(firstSchema: Schema, secondSchema: Schema): Try[Schema] = {
    val oldFields = firstSchema.fields.map(e => e.name -> e).toMap

    val notCompatibleFields = secondSchema.fields.collect {
      case field if oldFields.get(field.name).isDefined && oldFields(field.name).indexType != field.indexType =>
        s"mismatch type for field ${field.name} : new type ${field.indexType} is incompatible with old type"
    }

    if (notCompatibleFields.nonEmpty)
      Failure(new RuntimeException(notCompatibleFields.mkString(",")))
    else {
      val schema = Schema(secondSchema.metric, firstSchema.fields ++ secondSchema.fields)
      Success(schema)
    }
  }
}
