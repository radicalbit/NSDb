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
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.TimeSeriesIndex
import io.radicalbit.nsdb.model.Schema
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.Term
import org.apache.lucene.search.{MatchAllDocsQuery, TermQuery}
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class FirstLastIndexSpec extends FlatSpec with Matchers with OneInstancePerTest {

  private val BigIntValueSchema = Schema("testMetric",
                                         Bit(0,
                                             0,
                                             Map("dimension" -> "dimension"),
                                             Map(
                                               "tag1" -> "tag1",
                                               "tag2" -> 0L,
                                               "tag3" -> 0
                                             )))

  private val DecimalValueSchema = Schema("testMetric",
                                          Bit(0,
                                              1.1,
                                              Map("dimension" -> "dimension"),
                                              Map(
                                                "tag1" -> "tag1",
                                                "tag2" -> 0L,
                                                "tag3" -> 0
                                              )))

  "TimeSeriesIndex" should "return last values properly for a bigint value" in {

    val timeSeriesIndex =
      new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_first_last_index", UUID.randomUUID().toString)))

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

    val lastTag1 = timeSeriesIndex.getLastGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag1")
    lastTag1.size shouldBe 11
    lastTag1 should contain(Bit(10, 10, Map.empty, Map("tag1"  -> NSDbType("tag_0"))))
    lastTag1 should contain(Bit(20, 10, Map.empty, Map("tag1"  -> NSDbType("tag_1"))))
    lastTag1 should contain(Bit(30, 10, Map.empty, Map("tag1"  -> NSDbType("tag_2"))))
    lastTag1 should contain(Bit(40, 10, Map.empty, Map("tag1"  -> NSDbType("tag_3"))))
    lastTag1 should contain(Bit(50, 10, Map.empty, Map("tag1"  -> NSDbType("tag_4"))))
    lastTag1 should contain(Bit(60, 10, Map.empty, Map("tag1"  -> NSDbType("tag_5"))))
    lastTag1 should contain(Bit(70, 10, Map.empty, Map("tag1"  -> NSDbType("tag_6"))))
    lastTag1 should contain(Bit(80, 10, Map.empty, Map("tag1"  -> NSDbType("tag_7"))))
    lastTag1 should contain(Bit(90, 10, Map.empty, Map("tag1"  -> NSDbType("tag_8"))))
    lastTag1 should contain(Bit(100, 10, Map.empty, Map("tag1" -> NSDbType("tag_9"))))
    lastTag1 should contain(Bit(110, 10, Map.empty, Map("tag1" -> NSDbType("tag_10"))))

    val firstTag1 = timeSeriesIndex.getFirstGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag1")
    firstTag1.size shouldBe 11
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_0"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_1"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_2"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_3"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_4"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_5"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_6"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_7"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_8"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_9"))))
    firstTag1 should contain(Bit(0, 0, Map.empty, Map("tag1" -> NSDbType("tag_10"))))

    val lastTag2 = timeSeriesIndex.getLastGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag2")
    lastTag2.size shouldBe 11
    lastTag2 should contain(Bit(10, 10, Map.empty, Map("tag2"  -> NSDbType(0L))))
    lastTag2 should contain(Bit(20, 10, Map.empty, Map("tag2"  -> NSDbType(1L))))
    lastTag2 should contain(Bit(30, 10, Map.empty, Map("tag2"  -> NSDbType(2L))))
    lastTag2 should contain(Bit(40, 10, Map.empty, Map("tag2"  -> NSDbType(3L))))
    lastTag2 should contain(Bit(50, 10, Map.empty, Map("tag2"  -> NSDbType(4L))))
    lastTag2 should contain(Bit(60, 10, Map.empty, Map("tag2"  -> NSDbType(5L))))
    lastTag2 should contain(Bit(70, 10, Map.empty, Map("tag2"  -> NSDbType(6L))))
    lastTag2 should contain(Bit(80, 10, Map.empty, Map("tag2"  -> NSDbType(7L))))
    lastTag2 should contain(Bit(90, 10, Map.empty, Map("tag2"  -> NSDbType(8L))))
    lastTag2 should contain(Bit(100, 10, Map.empty, Map("tag2" -> NSDbType(9L))))
    lastTag2 should contain(Bit(110, 10, Map.empty, Map("tag2" -> NSDbType(10L))))

    val firstTag2 = timeSeriesIndex.getFirstGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag2")
    firstTag2.size shouldBe 11
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(0L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(1L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(2L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(3L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(4L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(5L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(6L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(7L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(8L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(9L))))
    firstTag2 should contain(Bit(0, 0, Map.empty, Map("tag2" -> NSDbType(10L))))

    val lastTag3 = timeSeriesIndex.getLastGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag3")
    lastTag3.size shouldBe 11
    lastTag3 should contain(Bit(10, 10, Map.empty, Map("tag3"  -> NSDbType(0.2))))
    lastTag3 should contain(Bit(20, 10, Map.empty, Map("tag3"  -> NSDbType(1.2))))
    lastTag3 should contain(Bit(30, 10, Map.empty, Map("tag3"  -> NSDbType(2.2))))
    lastTag3 should contain(Bit(40, 10, Map.empty, Map("tag3"  -> NSDbType(3.2))))
    lastTag3 should contain(Bit(50, 10, Map.empty, Map("tag3"  -> NSDbType(4.2))))
    lastTag3 should contain(Bit(60, 10, Map.empty, Map("tag3"  -> NSDbType(5.2))))
    lastTag3 should contain(Bit(70, 10, Map.empty, Map("tag3"  -> NSDbType(6.2))))
    lastTag3 should contain(Bit(80, 10, Map.empty, Map("tag3"  -> NSDbType(7.2))))
    lastTag3 should contain(Bit(90, 10, Map.empty, Map("tag3"  -> NSDbType(8.2))))
    lastTag3 should contain(Bit(100, 10, Map.empty, Map("tag3" -> NSDbType(9.2))))
    lastTag3 should contain(Bit(110, 10, Map.empty, Map("tag3" -> NSDbType(10.2))))

    val firstTag3 = timeSeriesIndex.getFirstGroupBy(new MatchAllDocsQuery, BigIntValueSchema, "tag3")
    firstTag3.size shouldBe 11
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(0.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(1.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(2.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(3.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(4.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(5.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(6.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(7.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(8.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(9.2))))
    firstTag3 should contain(Bit(0, 0, Map.empty, Map("tag3" -> NSDbType(10.2))))
  }

  "TimeSeriesIndex" should "return last values properly for a decimal value" in {

    val timeSeriesIndex =
      new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_first_last_index", UUID.randomUUID().toString)))

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

    val lastTag1 = timeSeriesIndex.getLastGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag1")
    lastTag1.size shouldBe 11
    lastTag1 should contain(Bit(10, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_0"))))
    lastTag1 should contain(Bit(20, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_1"))))
    lastTag1 should contain(Bit(30, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_2"))))
    lastTag1 should contain(Bit(40, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_3"))))
    lastTag1 should contain(Bit(50, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_4"))))
    lastTag1 should contain(Bit(60, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_5"))))
    lastTag1 should contain(Bit(70, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_6"))))
    lastTag1 should contain(Bit(80, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_7"))))
    lastTag1 should contain(Bit(90, 10.1, Map.empty, Map("tag1"  -> NSDbType("tag_8"))))
    lastTag1 should contain(Bit(100, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_9"))))
    lastTag1 should contain(Bit(110, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_10"))))

    val firstTag1 = timeSeriesIndex.getFirstGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag1")
    firstTag1.size shouldBe 11
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_0"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_1"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_2"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_3"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_4"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_5"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_6"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_7"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_8"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_9"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_10"))))

    val lastTag2 = timeSeriesIndex.getLastGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag2")
    lastTag2.size shouldBe 11
    lastTag2 should contain(Bit(10, 10.1, Map.empty, Map("tag2"  -> NSDbType(0L))))
    lastTag2 should contain(Bit(20, 10.1, Map.empty, Map("tag2"  -> NSDbType(1L))))
    lastTag2 should contain(Bit(30, 10.1, Map.empty, Map("tag2"  -> NSDbType(2L))))
    lastTag2 should contain(Bit(40, 10.1, Map.empty, Map("tag2"  -> NSDbType(3L))))
    lastTag2 should contain(Bit(50, 10.1, Map.empty, Map("tag2"  -> NSDbType(4L))))
    lastTag2 should contain(Bit(60, 10.1, Map.empty, Map("tag2"  -> NSDbType(5L))))
    lastTag2 should contain(Bit(70, 10.1, Map.empty, Map("tag2"  -> NSDbType(6L))))
    lastTag2 should contain(Bit(80, 10.1, Map.empty, Map("tag2"  -> NSDbType(7L))))
    lastTag2 should contain(Bit(90, 10.1, Map.empty, Map("tag2"  -> NSDbType(8L))))
    lastTag2 should contain(Bit(100, 10.1, Map.empty, Map("tag2" -> NSDbType(9L))))
    lastTag2 should contain(Bit(110, 10.1, Map.empty, Map("tag2" -> NSDbType(10L))))

    val firstTag2 = timeSeriesIndex.getFirstGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag2")
    firstTag2.size shouldBe 11
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(0L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(1L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(2L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(3L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(4L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(5L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(6L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(7L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(8L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(9L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(10L))))

    val lastTag3 = timeSeriesIndex.getLastGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag3")
    lastTag3.size shouldBe 11
    lastTag3 should contain(Bit(10, 10.1, Map.empty, Map("tag3"  -> NSDbType(0.2))))
    lastTag3 should contain(Bit(20, 10.1, Map.empty, Map("tag3"  -> NSDbType(1.2))))
    lastTag3 should contain(Bit(30, 10.1, Map.empty, Map("tag3"  -> NSDbType(2.2))))
    lastTag3 should contain(Bit(40, 10.1, Map.empty, Map("tag3"  -> NSDbType(3.2))))
    lastTag3 should contain(Bit(50, 10.1, Map.empty, Map("tag3"  -> NSDbType(4.2))))
    lastTag3 should contain(Bit(60, 10.1, Map.empty, Map("tag3"  -> NSDbType(5.2))))
    lastTag3 should contain(Bit(70, 10.1, Map.empty, Map("tag3"  -> NSDbType(6.2))))
    lastTag3 should contain(Bit(80, 10.1, Map.empty, Map("tag3"  -> NSDbType(7.2))))
    lastTag3 should contain(Bit(90, 10.1, Map.empty, Map("tag3"  -> NSDbType(8.2))))
    lastTag3 should contain(Bit(100, 10.1, Map.empty, Map("tag3" -> NSDbType(9.2))))
    lastTag3 should contain(Bit(110, 10.1, Map.empty, Map("tag3" -> NSDbType(10.2))))

    val firstTag3 = timeSeriesIndex.getFirstGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag3")
    firstTag3.size shouldBe 11
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(0.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(1.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(2.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(3.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(4.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(5.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(6.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(7.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(8.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(9.2))))
    firstTag3 should contain(Bit(0, 0.1, Map.empty, Map("tag3" -> NSDbType(10.2))))
  }

  "TimeSeriesIndex" should "filter results according to a query" in {
    val timeSeriesIndex =
      new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_first_last_index", UUID.randomUUID().toString)))

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

    val lastTag1 = timeSeriesIndex.getLastGroupBy(new TermQuery(new Term("tag1", "tag_5")), DecimalValueSchema, "tag1")
    lastTag1.size shouldBe 1
    lastTag1 should contain(Bit(60, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_5"))))

    val firstTag1 =
      timeSeriesIndex.getFirstGroupBy(new TermQuery(new Term("tag1", "tag_3")), DecimalValueSchema, "tag1")
    firstTag1.size shouldBe 1
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_3"))))

    val lastTag2 = timeSeriesIndex.getLastGroupBy(LongPoint.newRangeQuery("tag2", 0L, 5L), DecimalValueSchema, "tag2")
    lastTag2.size shouldBe 6
    lastTag2 should contain(Bit(10, 10.1, Map.empty, Map("tag2" -> NSDbType(0L))))
    lastTag2 should contain(Bit(20, 10.1, Map.empty, Map("tag2" -> NSDbType(1L))))
    lastTag2 should contain(Bit(30, 10.1, Map.empty, Map("tag2" -> NSDbType(2L))))
    lastTag2 should contain(Bit(40, 10.1, Map.empty, Map("tag2" -> NSDbType(3L))))
    lastTag2 should contain(Bit(50, 10.1, Map.empty, Map("tag2" -> NSDbType(4L))))
    lastTag2 should contain(Bit(60, 10.1, Map.empty, Map("tag2" -> NSDbType(5L))))

    val firstTag2 = timeSeriesIndex.getFirstGroupBy(LongPoint.newRangeQuery("tag2", 5L, 9L), DecimalValueSchema, "tag2")
    firstTag2.size shouldBe 5
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(5L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(6L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(7L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(8L))))
    firstTag2 should contain(Bit(0, 0.1, Map.empty, Map("tag2" -> NSDbType(9L))))
  }

  "TimeSeriesIndex" should "properly discard records with null group key in first and last aggregations" in {
    val timeSeriesIndex =
      new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_first_last_index", UUID.randomUUID().toString)))

    val writer = timeSeriesIndex.getWriter

    (0 to 5).foreach { j =>
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
    (6 to 10).foreach { j =>
      (0 to 10).foreach { i =>
        val testData =
          Bit(
            timestamp = i * (j + 1),
            value = i + 0.1,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag2"            -> NSDbType(j.toLong), "tag3" -> NSDbType(j + 0.2))
          )
        timeSeriesIndex.write(testData)(writer).get
      }
    }
    writer.close()

    val lastTag1 = timeSeriesIndex.getLastGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag1")
    lastTag1.size shouldBe 6
    lastTag1 should contain(Bit(10, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_0"))))
    lastTag1 should contain(Bit(20, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_1"))))
    lastTag1 should contain(Bit(30, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_2"))))
    lastTag1 should contain(Bit(40, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_3"))))
    lastTag1 should contain(Bit(50, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_4"))))
    lastTag1 should contain(Bit(60, 10.1, Map.empty, Map("tag1" -> NSDbType("tag_5"))))

    val firstTag1 =
      timeSeriesIndex.getFirstGroupBy(new MatchAllDocsQuery, DecimalValueSchema, "tag1")
    firstTag1.size shouldBe 6
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_0"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_1"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_2"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_3"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_4"))))
    firstTag1 should contain(Bit(0, 0.1, Map.empty, Map("tag1" -> NSDbType("tag_5"))))

  }

}
