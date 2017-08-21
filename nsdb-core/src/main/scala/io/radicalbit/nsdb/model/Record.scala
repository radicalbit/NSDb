package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.index.IndexType

case class RawField(name: String, value: JSerializable)
case class TypedField(name: String, indexType: IndexType[_], value: JSerializable)
case class SchemaField(name: String, indexType: IndexType[_])
