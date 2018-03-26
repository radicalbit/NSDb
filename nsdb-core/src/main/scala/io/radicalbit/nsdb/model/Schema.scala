package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.{IndexType, TypeSupport}

import scala.util.Try

case class RawField(name: String, value: JSerializable)
case class TypedField(name: String, indexType: IndexType[_], value: JSerializable)
case class SchemaField(name: String, indexType: IndexType[_])

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
  def apply(metric: String, bit: Bit): Try[Schema] = {
    validateSchemaTypeSupport(bit).map((fields: Seq[TypedField]) =>
      Schema(metric, fields.map(field => SchemaField(field.name, field.indexType)).toSet))
  }
}