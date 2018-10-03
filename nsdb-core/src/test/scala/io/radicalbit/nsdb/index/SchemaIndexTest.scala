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

package io.radicalbit.nsdb.index

import io.radicalbit.nsdb.common.protocol.{DimensionFieldType, TagFieldType}
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import org.apache.lucene.store.RAMDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

import scala.util.Success

class SchemaIndexTest extends FlatSpec with Matchers with OneInstancePerTest {

  "SchemaIndex" should "union schemas properly" in {

    val schema1 = Schema(
      s"metric",
      Set(
        SchemaField("field1", DimensionFieldType, BIGINT()),
        SchemaField("field2", TagFieldType, VARCHAR()),
        SchemaField(s"field3", DimensionFieldType, VARCHAR())
      )
    )

    val schema2 = Schema(
      s"metric",
      Set(
        SchemaField("field1", DimensionFieldType, BIGINT()),
        SchemaField("field2", TagFieldType, VARCHAR()),
      )
    )

    SchemaIndex.union(schema1, schema2) shouldBe Success(schema1)

  }

  "SchemaIndex" should "write and read properly" in {

    lazy val directory = new RAMDirectory()

    val schemaIndex = new SchemaIndex(directory)

    implicit val writer = schemaIndex.getWriter

    (0 to 100).foreach { i =>
      val testData = Schema(
        s"metric_$i",
        Set(
          SchemaField("field1", DimensionFieldType, BIGINT()),
          SchemaField("field2", DimensionFieldType, VARCHAR()),
          SchemaField(s"field$i", DimensionFieldType, VARCHAR())
        )
      )
      schemaIndex.write(testData)
    }
    writer.close()

    val result = schemaIndex.query(schemaIndex._keyField, "metric_*", Seq.empty, 100)(identity)

    result.size shouldBe 100

    val firstSchema = schemaIndex.getSchema("metric_0")

    firstSchema shouldBe Some(
      Schema(
        s"metric_0",
        Set(
          SchemaField("field1", DimensionFieldType, BIGINT()),
          SchemaField("field2", DimensionFieldType, VARCHAR()),
          SchemaField(s"field0", DimensionFieldType, VARCHAR())
        )
      ))
  }

  "SchemaIndex" should "update records" in {

    val testData = Schema(
      "metric_1",
      Set(SchemaField("field1", DimensionFieldType, BIGINT()), SchemaField("field2", DimensionFieldType, VARCHAR())))
    val testData2 = Schema(
      "metric_1",
      Set(SchemaField("field1", DimensionFieldType, INT()), SchemaField("field2", DimensionFieldType, VARCHAR())))

    lazy val directory = new RAMDirectory()

    val schemaIndex = new SchemaIndex(directory)

    implicit val writer = schemaIndex.getWriter

    schemaIndex.write(testData)
    writer.close()

    schemaIndex.getSchema("metric_1") shouldBe Some(testData)

    (1 to 100).foreach { i =>
      val writer2 = schemaIndex.getWriter
      schemaIndex.update("metric_1", testData2)(writer2)
      writer2.close()
      schemaIndex.refresh()
    }

    schemaIndex.all.size shouldBe 1

    schemaIndex.getSchema("metric_1") shouldBe Some(testData2)
  }

  "SchemaIndex" should "drop schema" in {

    lazy val directory = new RAMDirectory()

    val schemaIndex = new SchemaIndex(directory)

    implicit val writer = schemaIndex.getWriter

    val testData = Schema(
      s"metric_2",
      Set(SchemaField("field1", DimensionFieldType, BIGINT()), SchemaField("field2", DimensionFieldType, VARCHAR())))
    schemaIndex.write(testData)

    val testDataBis = Schema(
      s"metric_3",
      Set(SchemaField("field1", DimensionFieldType, BIGINT()), SchemaField("field2", DimensionFieldType, VARCHAR())))
    schemaIndex.write(testDataBis)
    writer.close()

    implicit val writerDrop = schemaIndex.getWriter

    val result = schemaIndex.getSchema("metric_2")

    result shouldBe Some(testData)

    schemaIndex.delete(testData)(writerDrop)

    writerDrop.close()

    schemaIndex.refresh()

    schemaIndex.getSchema("metric_2") shouldBe None
    schemaIndex.getSchema("metric_3") shouldBe Some(testDataBis)

  }

}
