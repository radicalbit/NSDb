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
    * create a schema by analyzing a [[Bit]] structure.
    * @param metric the metric.
    * @param bit the bit to be used to create the schema.
    * @return the resulting [[Schema]]. If bit contains invalid fields the result will be a [[scala.util.Failure]]
    */
  def apply(metric: String, bit: Bit): Try[Schema] = {
    validateSchemaTypeSupport(bit).map((fields: Seq[TypedField]) =>
      Schema(metric, fields.map(field => SchemaField(field.name, field.indexType)).toSet))
  }
}
