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

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index.lucene.MaxAllGroupsCollector
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class TimeSeriesIndexTest extends FlatSpec with Matchers with OneInstancePerTest {

  "TimeSeriesIndex" should "write and read properly on disk" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    implicit val writer = timeSeriesIndex.getWriter

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = System.currentTimeMillis,
            value = 23,
            dimensions = Map("content" -> s"content_$i"),
            tags = Map.empty)
      timeSeriesIndex.write(testData)
    }
    writer.close()

    val result = timeSeriesIndex.query("content", "content_*", Seq.empty, 100)(identity)

    result.size shouldBe 100

  }

  "TimeSeriesIndex" should "support values containing dashes" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    implicit val writer = timeSeriesIndex.getWriter

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i, value = 23.5, dimensions = Map("content" -> s"content-$i"), tags = Map.empty)
      timeSeriesIndex.write(testData)
    }

    writer.close()

    val query = new TermQuery(new Term("content", "content-10"))

    implicit val searcher = timeSeriesIndex.getSearcher
    val result            = timeSeriesIndex.rawQuery(query, 100, Some(new Sort(new SortField("timestamp", SortField.Type.DOC))))

    result.size shouldBe 1

    val wildcardQuery = new WildcardQuery(new Term("content", "content-10*"))
    val wildcardResult =
      timeSeriesIndex.rawQuery(wildcardQuery, 100, Some(new Sort(new SortField("timestamp", SortField.Type.DOC))))

    wildcardResult.size shouldBe 2
  }

  "TimeSeriesIndex" should "support range queries and sorting" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    implicit val writer = timeSeriesIndex.getWriter

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i, value = 23.5, dimensions = Map("content" -> s"content_$i"), tags = Map.empty)
      timeSeriesIndex.write(testData)
    }

    writer.close()

    val query = LongPoint.newRangeQuery("timestamp", 10, 20)

    implicit val searcher = timeSeriesIndex.getSearcher
    val result            = timeSeriesIndex.rawQuery(query, 100, Some(new Sort(new SortField("timestamp", SortField.Type.DOC))))

    result.size shouldBe 11

    (1 to 10).foreach { i =>
      result(i).getField("timestamp").numericValue().longValue should be >= result(i - 1)
        .getField("timestamp")
        .numericValue
        .longValue
    }
  }

  "TimeSeriesIndex" should "delete records" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    implicit val writer = timeSeriesIndex.getWriter

    val timestamp = System.currentTimeMillis

    val testData = Bit(timestamp = timestamp, value = 0.2, dimensions = Map("content" -> s"content"), tags = Map.empty)

    timeSeriesIndex.write(testData)

    writer.close()

    val queryExist = LongPoint.newExactQuery("timestamp", timestamp)
    val resultExist =
      timeSeriesIndex.query(queryExist, Seq.empty, 100, None)(identity)
    resultExist.size shouldBe 1

    val deleteWriter = timeSeriesIndex.getWriter
    timeSeriesIndex.delete(testData)(deleteWriter)

    deleteWriter.close()

    timeSeriesIndex.refresh()

    val query = LongPoint.newExactQuery("timestamp", timestamp)
    val result =
      timeSeriesIndex.query(query, Seq.empty, 100, None)(identity)

    result.size shouldBe 0

  }

  "TimeSeriesIndex" should "support groupBy queries" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    implicit val writer = timeSeriesIndex.getWriter

    (0 to 9).foreach { i =>
      val testData = Bit(timestamp = System.currentTimeMillis,
                         value = 10,
                         dimensions = Map("content" -> s"content_${i / 4}", "number" -> i),
                         tags = Map.empty)
      timeSeriesIndex.write(testData)
    }

    writer.close()

    val collector = new MaxAllGroupsCollector[Long, String]("content", "number")

    timeSeriesIndex.getSearcher.search(new MatchAllDocsQuery(), collector)

    collector.getGroupCount shouldBe 3
    val sorted = collector.getGroupMap.toSeq.sortBy(_._2)
    sorted shouldBe Seq(("content_0", 3), ("content_1", 7), ("content_2", 9))
  }

  "TimeSeriesIndex" should "support groupBy queries with ordering" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    val records: Seq[Bit] = (0 to 9).map { i =>
      Bit(timestamp = i, value = i, dimensions = Map("content" -> s"content_${i / 4}"), tags = Map.empty)
    }

    implicit val writer = timeSeriesIndex.getWriter
    records.foreach(timeSeriesIndex.write)
    writer.close()

    val ascSort = new Sort(new SortField("value", SortField.Type.INT, false))

    val results = timeSeriesIndex.query(new MatchAllDocsQuery(),
                                        new MaxAllGroupsCollector[Long, String]("content", "value"),
                                        None,
                                        Some(ascSort))

    results shouldBe Seq(
      Bit(0, 3, Map("content" -> "content_0"), tags = Map.empty),
      Bit(0, 7, Map("content" -> "content_1"), tags = Map.empty),
      Bit(0, 9, Map("content" -> "content_2"), tags = Map.empty)
    )

    val descSort = new Sort(new SortField("value", SortField.Type.INT, true))

    val descResults = timeSeriesIndex.query(new MatchAllDocsQuery(),
                                            new MaxAllGroupsCollector[Long, String]("content", "value"),
                                            None,
                                            Some(descSort))

    descResults shouldBe Seq(
      Bit(0, 9, Map("content" -> "content_2"), tags = Map.empty),
      Bit(0, 7, Map("content" -> "content_1"), tags = Map.empty),
      Bit(0, 3, Map("content" -> "content_0"), tags = Map.empty)
    )
  }

  "TimeSeriesIndex" should "support groupBy queries with ordering and limiting" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    val records: Seq[Bit] = (0 to 9).map { i =>
      Bit(timestamp = i, value = i, dimensions = Map("content" -> s"content_${i / 4}"), tags = Map.empty)
    }

    implicit val writer = timeSeriesIndex.getWriter
    records.foreach(timeSeriesIndex.write)
    writer.close()

    val descSort = new Sort(new SortField("value", SortField.Type.INT, true))

    val descResults = timeSeriesIndex.query(new MatchAllDocsQuery(),
                                            new MaxAllGroupsCollector[Long, String]("content", "value"),
                                            Some(2),
                                            Some(descSort))

    descResults shouldBe Seq(Bit(0, 9, Map("content" -> "content_2"), tags = Map.empty),
                             Bit(0, 7, Map("content" -> "content_1"), tags = Map.empty))
  }
}
