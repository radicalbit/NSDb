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

package io.radicalbit.nsdb.index.aux

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.common.NSDbType
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType, TagFieldType, TimestampFieldType}
import io.radicalbit.nsdb.index.{BIGINT, DECIMAL, TimeSeriesIndex, VARCHAR}
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.Term
import org.apache.lucene.search.{MatchAllDocsQuery, TermQuery}
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class MinMaxIndexSpec extends FlatSpec with Matchers with OneInstancePerTest {
  private val BigIntValueSchema = Schema(
    "testMetric",
    Map(
      "dimension" -> SchemaField("dimension", DimensionFieldType, VARCHAR()),
      "tag1"      -> SchemaField("tag1", TagFieldType, VARCHAR()),
      "tag2"      -> SchemaField("tag2", TagFieldType, BIGINT()),
      "tag3"      -> SchemaField("tag3", TagFieldType, DECIMAL()),
      "value"     -> SchemaField("value", TagFieldType, BIGINT()),
      "timestamp" -> SchemaField("timestamp", TimestampFieldType, BIGINT())
    )
  )

  private val DecimalValueSchema = Schema(
    "testMetric",
    Map(
      "dimension" -> SchemaField("dimension", DimensionFieldType, VARCHAR()),
      "tag1"      -> SchemaField("tag1", TagFieldType, VARCHAR()),
      "tag2"      -> SchemaField("tag2", TagFieldType, BIGINT()),
      "tag3"      -> SchemaField("tag3", TagFieldType, DECIMAL()),
      "value"     -> SchemaField("value", TagFieldType, DECIMAL()),
      "timestamp" -> SchemaField("timestamp", TimestampFieldType, BIGINT())
    )
  )

  "TimeSeriesIndex" should "return max values properly for a bigint value" in {

    val timeSeriesIndex =
      new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_min_max_index", UUID.randomUUID().toString)))

    val writer = timeSeriesIndex.getWriter

    (0 to 10).foreach { j =>
      (0 to 10).foreach { i =>
        val testData =
          Bit(
            timestamp = i * (j + 1),
            value = i,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag1"            -> NSDbType(s"tag_$j"), "tag2" -> NSDbType(j.toLong), "tag3" -> NSDbType(j + 0.2))
          )
        timeSeriesIndex.write(testData)(writer)
      }
    }
    writer.close()

    val maxTag1 = timeSeriesIndex.getMaxGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag1")
    maxTag1.size shouldBe 11
    maxTag1 should contain(Bit(10, 10, Map.empty, Map("tag1"  -> NSDbType("tag_0"))))
    maxTag1 should contain(Bit(20, 10, Map.empty, Map("tag1"  -> NSDbType("tag_1"))))
    maxTag1 should contain(Bit(30, 10, Map.empty, Map("tag1"  -> NSDbType("tag_2"))))
    maxTag1 should contain(Bit(40, 10, Map.empty, Map("tag1"  -> NSDbType("tag_3"))))
    maxTag1 should contain(Bit(50, 10, Map.empty, Map("tag1"  -> NSDbType("tag_4"))))
    maxTag1 should contain(Bit(60, 10, Map.empty, Map("tag1"  -> NSDbType("tag_5"))))
    maxTag1 should contain(Bit(70, 10, Map.empty, Map("tag1"  -> NSDbType("tag_6"))))
    maxTag1 should contain(Bit(80, 10, Map.empty, Map("tag1"  -> NSDbType("tag_7"))))
    maxTag1 should contain(Bit(90, 10, Map.empty, Map("tag1"  -> NSDbType("tag_8"))))
    maxTag1 should contain(Bit(100, 10, Map.empty, Map("tag1" -> NSDbType("tag_9"))))
    maxTag1 should contain(Bit(110, 10, Map.empty, Map("tag1" -> NSDbType("tag_10"))))

    val minTag1 = timeSeriesIndex.getMinGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag1")
    minTag1.size shouldBe 11
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_0"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_1"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_2"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_3"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_4"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_5"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_6"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_7"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_8"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_9"))))
    minTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_10"))))

    val maxTag2 = timeSeriesIndex.getMaxGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag2")
    maxTag2.size shouldBe 11
    maxTag2 should contain(Bit(10, 10, Map.empty, Map("tag2"  -> NSDbType(0L))))
    maxTag2 should contain(Bit(20, 10, Map.empty, Map("tag2"  -> NSDbType(1L))))
    maxTag2 should contain(Bit(30, 10, Map.empty, Map("tag2"  -> NSDbType(2L))))
    maxTag2 should contain(Bit(40, 10, Map.empty, Map("tag2"  -> NSDbType(3L))))
    maxTag2 should contain(Bit(50, 10, Map.empty, Map("tag2"  -> NSDbType(4L))))
    maxTag2 should contain(Bit(60, 10, Map.empty, Map("tag2"  -> NSDbType(5L))))
    maxTag2 should contain(Bit(70, 10, Map.empty, Map("tag2"  -> NSDbType(6L))))
    maxTag2 should contain(Bit(80, 10, Map.empty, Map("tag2"  -> NSDbType(7L))))
    maxTag2 should contain(Bit(90, 10, Map.empty, Map("tag2"  -> NSDbType(8L))))
    maxTag2 should contain(Bit(100, 10, Map.empty, Map("tag2" -> NSDbType(9L))))
    maxTag2 should contain(Bit(110, 10, Map.empty, Map("tag2" -> NSDbType(10L))))

    val minTag2 = timeSeriesIndex.getMinGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag2")
    minTag2.size shouldBe 11
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(0L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(1L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(2L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(3L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(4L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(5L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(6L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(7L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(8L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(9L))))
    minTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(10L))))

    val maxTag3 = timeSeriesIndex.getMaxGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag3")
    maxTag3.size shouldBe 11
    maxTag3 should contain(Bit(10, 10, Map.empty, Map("tag3"  -> NSDbType(0.2))))
    maxTag3 should contain(Bit(20, 10, Map.empty, Map("tag3"  -> NSDbType(1.2))))
    maxTag3 should contain(Bit(30, 10, Map.empty, Map("tag3"  -> NSDbType(2.2))))
    maxTag3 should contain(Bit(40, 10, Map.empty, Map("tag3"  -> NSDbType(3.2))))
    maxTag3 should contain(Bit(50, 10, Map.empty, Map("tag3"  -> NSDbType(4.2))))
    maxTag3 should contain(Bit(60, 10, Map.empty, Map("tag3"  -> NSDbType(5.2))))
    maxTag3 should contain(Bit(70, 10, Map.empty, Map("tag3"  -> NSDbType(6.2))))
    maxTag3 should contain(Bit(80, 10, Map.empty, Map("tag3"  -> NSDbType(7.2))))
    maxTag3 should contain(Bit(90, 10, Map.empty, Map("tag3"  -> NSDbType(8.2))))
    maxTag3 should contain(Bit(100, 10, Map.empty, Map("tag3" -> NSDbType(9.2))))
    maxTag3 should contain(Bit(110, 10, Map.empty, Map("tag3" -> NSDbType(10.2))))

    val minTag3 = timeSeriesIndex.getMinGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag3")
    minTag3.size shouldBe 11
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(0.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(1.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(2.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(3.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(4.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(5.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(6.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(7.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(8.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(9.2))))
    minTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(10.2))))
  }

  "TimeSeriesIndex" should "return max values properly for a decimal value" in {

    val timeSeriesIndex =
      new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_min_max_index", UUID.randomUUID().toString)))

    val writer = timeSeriesIndex.getWriter

    (0 to 10).foreach { j =>
      (0 to 10).foreach { i =>
        val testData =
          Bit(
            timestamp = i * (j + 1),
            value = i + 0.1,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag1"            -> NSDbType(s"tag_$j"), "tag2" -> NSDbType(j.toLong), "tag3" -> NSDbType(j + 0.2))
          )
        timeSeriesIndex.write(testData)(writer).get
      }
    }
    writer.close()

    val maxTag1 = timeSeriesIndex.getMaxGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag1")
    maxTag1.size shouldBe 11
    maxTag1 should contain(Bit(10, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_0"))))
    maxTag1 should contain(Bit(20, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_1"))))
    maxTag1 should contain(Bit(30, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_2"))))
    maxTag1 should contain(Bit(40, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_3"))))
    maxTag1 should contain(Bit(50, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_4"))))
    maxTag1 should contain(Bit(60, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_5"))))
    maxTag1 should contain(Bit(70, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_6"))))
    maxTag1 should contain(Bit(80, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_7"))))
    maxTag1 should contain(Bit(90, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_8"))))
    maxTag1 should contain(Bit(100, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_9"))))
    maxTag1 should contain(Bit(110, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_10"))))

    val minTag1 = timeSeriesIndex.getMinGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag1")
    minTag1.size shouldBe 11
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_0"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_1"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_2"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_3"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_4"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_5"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_6"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_7"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_8"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_9"))))
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_10"))))

    val maxTag2 = timeSeriesIndex.getMaxGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag2")
    maxTag2.size shouldBe 11
    maxTag2 should contain(Bit(10, 10.1, Map.empty, Map("tag2"  -> NSDbType(0L))))
    maxTag2 should contain(Bit(20, 10.1, Map.empty, Map("tag2"  -> NSDbType(1L))))
    maxTag2 should contain(Bit(30, 10.1, Map.empty, Map("tag2"  -> NSDbType(2L))))
    maxTag2 should contain(Bit(40, 10.1, Map.empty, Map("tag2"  -> NSDbType(3L))))
    maxTag2 should contain(Bit(50, 10.1, Map.empty, Map("tag2"  -> NSDbType(4L))))
    maxTag2 should contain(Bit(60, 10.1, Map.empty, Map("tag2"  -> NSDbType(5L))))
    maxTag2 should contain(Bit(70, 10.1, Map.empty, Map("tag2"  -> NSDbType(6L))))
    maxTag2 should contain(Bit(80, 10.1, Map.empty, Map("tag2"  -> NSDbType(7L))))
    maxTag2 should contain(Bit(90, 10.1, Map.empty, Map("tag2"  -> NSDbType(8L))))
    maxTag2 should contain(Bit(100, 10.1, Map.empty, Map("tag2" -> NSDbType(9L))))
    maxTag2 should contain(Bit(110, 10.1, Map.empty, Map("tag2" -> NSDbType(10L))))

    val minTag2 = timeSeriesIndex.getMinGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag2")
    minTag2.size shouldBe 11
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(0L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(1L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(2L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(3L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(4L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(5L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(6L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(7L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(8L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(9L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(10L))))

    val maxTag3 = timeSeriesIndex.getMaxGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag3")
    maxTag3.size shouldBe 11
    maxTag3 should contain(Bit(10, 10.1, Map.empty, Map("tag3"  -> NSDbType(0.2))))
    maxTag3 should contain(Bit(20, 10.1, Map.empty, Map("tag3"  -> NSDbType(1.2))))
    maxTag3 should contain(Bit(30, 10.1, Map.empty, Map("tag3"  -> NSDbType(2.2))))
    maxTag3 should contain(Bit(40, 10.1, Map.empty, Map("tag3"  -> NSDbType(3.2))))
    maxTag3 should contain(Bit(50, 10.1, Map.empty, Map("tag3"  -> NSDbType(4.2))))
    maxTag3 should contain(Bit(60, 10.1, Map.empty, Map("tag3"  -> NSDbType(5.2))))
    maxTag3 should contain(Bit(70, 10.1, Map.empty, Map("tag3"  -> NSDbType(6.2))))
    maxTag3 should contain(Bit(80, 10.1, Map.empty, Map("tag3"  -> NSDbType(7.2))))
    maxTag3 should contain(Bit(90, 10.1, Map.empty, Map("tag3"  -> NSDbType(8.2))))
    maxTag3 should contain(Bit(100, 10.1, Map.empty, Map("tag3" -> NSDbType(9.2))))
    maxTag3 should contain(Bit(110, 10.1, Map.empty, Map("tag3" -> NSDbType(10.2))))

    val minTag3 = timeSeriesIndex.getMinGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag3")
    minTag3.size shouldBe 11
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(0.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(1.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(2.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(3.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(4.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(5.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(6.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(7.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(8.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(9.2))))
    minTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(10.2))))
  }

  "TimeSeriesIndex" should "filter results according to a query" in {
    val timeSeriesIndex =
      new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_min_max_index", UUID.randomUUID().toString)))

    val writer = timeSeriesIndex.getWriter

    (0 to 10).foreach { j =>
      (0 to 10).foreach { i =>
        val testData =
          Bit(
            timestamp = i * (j + 1),
            value = i + 0.1,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag1"            -> NSDbType(s"tag_$j"), "tag2" -> NSDbType(j.toLong), "tag3" -> NSDbType(j + 0.2))
          )
        timeSeriesIndex.write(testData)(writer).get
      }
    }
    writer.close()

    val maxTag1 = timeSeriesIndex.getMaxGroupBy(new TermQuery(new Term("tag1", "tag_5")), DecimalValueSchema, "tag1")
    maxTag1.size shouldBe 1
    maxTag1 should contain(Bit(60, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_5"))))

    val minTag1 =
      timeSeriesIndex.getMinGroupBy(new TermQuery(new Term("tag1", "tag_3")), DecimalValueSchema, "tag1")
    minTag1.size shouldBe 1
    minTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_3"))))

    val maxTag2 = timeSeriesIndex.getMaxGroupBy(LongPoint.newRangeQuery("tag2", 0L, 5L), DecimalValueSchema, "tag2")
    maxTag2.size shouldBe 6
    maxTag2 should contain(Bit(10, 10.1, Map.empty, Map("tag2" -> NSDbType(0L))))
    maxTag2 should contain(Bit(20, 10.1, Map.empty, Map("tag2" -> NSDbType(1L))))
    maxTag2 should contain(Bit(30, 10.1, Map.empty, Map("tag2" -> NSDbType(2L))))
    maxTag2 should contain(Bit(40, 10.1, Map.empty, Map("tag2" -> NSDbType(3L))))
    maxTag2 should contain(Bit(50, 10.1, Map.empty, Map("tag2" -> NSDbType(4L))))
    maxTag2 should contain(Bit(60, 10.1, Map.empty, Map("tag2" -> NSDbType(5L))))

    val minTag2 = timeSeriesIndex.getMinGroupBy(LongPoint.newRangeQuery("tag2", 5L, 9L), DecimalValueSchema, "tag2")
    minTag2.size shouldBe 5
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(5L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(6L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(7L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(8L))))
    minTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(9L))))
  }
}
