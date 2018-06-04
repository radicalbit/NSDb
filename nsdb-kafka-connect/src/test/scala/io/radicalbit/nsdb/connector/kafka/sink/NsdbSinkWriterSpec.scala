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

import io.radicalbit.nsdb.api.scala.{Bit, Db}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

import scala.util.Try

class NsdbSinkWriterSpec extends FlatSpec with Matchers with OneInstancePerTest {

  val simpleSchema = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("string_id", Schema.STRING_SCHEMA)
    .field("int_field", Schema.INT32_SCHEMA)
    .field("long_field", Schema.INT64_SCHEMA)
    .field("string_field", Schema.STRING_SCHEMA)
    .build

  val simpleSchemaWithDimensions = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("string_id", Schema.STRING_SCHEMA)
    .field("int_field", Schema.INT32_SCHEMA)
    .field("long_field", Schema.INT64_SCHEMA)
    .field("string_field", Schema.STRING_SCHEMA)
    .field("d1", Schema.STRING_SCHEMA)
    .field("d2", Schema.INT32_SCHEMA)
    .build

  val simpleStruct = new Struct(simpleSchema)
    .put("string_id", "my_id_val")
    .put("int_field", 12)
    .put("long_field", 12L)
    .put("string_field", "foo")

  val simpleStructWithDimensions = new Struct(simpleSchemaWithDimensions)
    .put("string_id", "my_id_val")
    .put("int_field", 12)
    .put("long_field", 12L)
    .put("string_field", "foo")
    .put("d1", "d1")
    .put("d2", 12)

  val schema = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("string_id", Schema.STRING_SCHEMA)
    .field("int_field", Schema.INT32_SCHEMA)
    .field("long_field", Schema.INT64_SCHEMA)
    .field("string_field", Schema.STRING_SCHEMA)
    .field("my_struct", simpleSchema)
    .build

  val struct = new Struct(schema)
    .put("string_id", "my_id_val")
    .put("int_field", 12)
    .put("long_field", 12L)
    .put("string_field", "foo")
    .put("my_struct", simpleStruct)

  val simpleRecord = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", simpleSchema, simpleStruct, 1)
  val simpleRecordWithDimentions =
    new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", simpleSchemaWithDimensions, simpleStructWithDimensions, 1)
  val record = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", schema, struct, 1)

  val stringRecord = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", schema, "", 1)

  "SinkRecordConversion" should "convert a struct SinkRecord to a Map" in {
    val mo = NsdbSinkWriter.parse(simpleRecord, None, None, None)

    mo.keys.size shouldBe 4
    mo.get("string_id") shouldBe Some("my_id_val")
    mo.get("int_field") shouldBe Some(12)
    mo.get("long_field") shouldBe Some(12)
  }

  "SinkRecordConversion" should "convert a SinkRecord with nested type to a Map" in {

    val mo = NsdbSinkWriter.parse(record, None, None, None)
    mo.keys.size shouldBe 8
    mo.get("string_id") shouldBe Some("my_id_val")
    mo.get("int_field") shouldBe Some(12)
    mo.get("long_field") shouldBe Some(12)
    mo.get("string_field") shouldBe Some("foo")

    mo.get("my_struct.string_id") shouldBe Some("my_id_val")
    mo.get("my_struct.int_field") shouldBe Some(12)
    mo.get("my_struct.long_field") shouldBe Some(12)
    mo.get("my_struct.string_field") shouldBe Some("foo")

  }

  "SinkRecordConversion" should "ignore non struct records" in {
    val stringRecord = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", schema, "a generic string", 1)
    an[RuntimeException] should be thrownBy NsdbSinkWriter.parse(stringRecord, None, None, None)
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and no global params" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, int_field AS value, d2, d1 FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, None)

    val bit: Bit =
      NsdbSinkWriter.convertToBit(parsedKcql, NsdbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val expectedBit =
      Db("my_id_val").namespace("foo").bit("metric").timestamp(12).value(12).dimension("d2", 12).dimension("d1", "d1")

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and global db" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_field AS namespace, int_field AS value FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, Some("globalDb"), None, None)

    val bit: Bit =
      NsdbSinkWriter.convertToBit(parsedKcql,
                                  NsdbSinkWriter.parse(simpleRecordWithDimentions, Some("globalDb"), None, None))

    val expectedBit =
      Db("globalDb").namespace("foo").bit("metric").timestamp(12).value(12)

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and global namespace" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, int_field AS value FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, Some("globalNs"), None)

    val bit: Bit =
      NsdbSinkWriter.convertToBit(parsedKcql,
                                  NsdbSinkWriter.parse(simpleRecordWithDimentions, None, Some("globalNs"), None))

    val expectedBit =
      Db("my_id_val").namespace("globalNs").bit("metric").timestamp(12).value(12)

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and global parameters" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT int_field AS value, d1 as dim1, d2 as dim2 FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, Some("globalDb"), Some("globalNs"), None)

    val bit: Bit =
      NsdbSinkWriter.convertToBit(
        parsedKcql,
        NsdbSinkWriter.parse(simpleRecordWithDimentions, Some("globalDb"), Some("globalNs"), None))

    val expectedBit =
      Db("globalDb")
        .namespace("globalNs")
        .bit("metric")
        .timestamp(12)
        .value(12)
        .dimension("dim1", "d1")
        .dimension("dim2", 12)

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql with and a default value (to be ignored)" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, int_field AS value, d2, d1 FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, Some("1"))

    val bit: Bit =
      NsdbSinkWriter.convertToBit(parsedKcql, NsdbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val expectedBit =
      Db("my_id_val").namespace("foo").bit("metric").timestamp(12).value(12).dimension("d2", 12).dimension("d1", "d1")

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql without a value alias and an invalid default value" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2, d1 FROM topic WITHTIMESTAMP long_field"

    val result = Try {

      val parsedKcql = ParsedKcql(withDimensionAlias, None, None, Some("1v"))

      NsdbSinkWriter.convertToBit(parsedKcql, NsdbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))
    }

    result.isFailure shouldBe true
  }

  "SinkRecordConversion" should "successfully convert records given a kcql without a value alias and a default value" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2, d1 FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, Some("1"))

    val bit: Bit =
      NsdbSinkWriter.convertToBit(parsedKcql, NsdbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val expectedBit =
      Db("my_id_val").namespace("foo").bit("metric").timestamp(12).value(1).dimension("d2", 12).dimension("d1", "d1")

    bit shouldBe expectedBit
  }

}
