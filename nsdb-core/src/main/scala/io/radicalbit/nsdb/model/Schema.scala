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
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}

import scala.util.Try

/**
  * It models a bit field
  * @param name field name.
  * @param value field value.
  */
case class RawField(name: String, value: JSerializable)

/**
  * It models a bit field decorated by an internal type descriptor.
  * @param name field name.
  * @param indexType type descriptor.
  * @param value field value.
  */
case class TypedField(name: String, indexType: IndexType[_], value: JSerializable)

/**
  * It models a schema field.
  * @param name field name.
  * @param indexType the internal type descriptor.
  */
case class SchemaField(name: String, indexType: IndexType[_])

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
      Schema(metric, fields.map(field => SchemaField(field.name, field.indexType)).toSet))
  }
}
