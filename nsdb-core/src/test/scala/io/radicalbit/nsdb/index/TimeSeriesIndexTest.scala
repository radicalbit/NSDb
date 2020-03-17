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

package io.radicalbit.nsdb.index

import java.nio.file.Paths
import java.util.UUID

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Schema
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class TimeSeriesIndexTest extends FlatSpec with Matchers with OneInstancePerTest {

  private val schema = Schema("", Bit(0, 0, Map("dimension" -> "d"), Map("tag" -> "t")))

  "TimeSeriesIndex" should "write and read properly on disk" in {

    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    val metricWriter = timeSeriesIndex.getWriter

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = System.currentTimeMillis,
            value = 23,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag"             -> s"tag_$i"))
      timeSeriesIndex.write(testData)(metricWriter)
    }
    metricWriter.close()

    timeSeriesIndex.query(schema, "dimension", "dimension_*", Seq.empty, 100)(identity).size shouldBe 100
    timeSeriesIndex.query(schema, "tag", "tag_*", Seq.empty, 100)(identity).size shouldBe 100
  }

  "TimeSeriesIndex" should "support values containing dashes" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    implicit val writer = timeSeriesIndex.getWriter

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i,
            value = 23.5,
            dimensions = Map("dimension" -> s"dimension-$i"),
            tags = Map("tag"             -> s"tag-$i"))
      timeSeriesIndex.write(testData)
    }

    writer.close()

    val query = new TermQuery(new Term("dimension", "dimension-10"))

    implicit val searcher = timeSeriesIndex.getSearcher
    val result            = timeSeriesIndex.rawQuery(query, 100, Some(new Sort(new SortField("timestamp", SortField.Type.DOC))))

    result.size shouldBe 1

    val wildcardQuery = new WildcardQuery(new Term("dimension", "dimension-10*"))
    val wildcardResult =
      timeSeriesIndex.rawQuery(wildcardQuery, 100, Some(new Sort(new SortField("timestamp", SortField.Type.DOC))))

    wildcardResult.size shouldBe 2
  }

  "TimeSeriesIndex" should "support range queries and sorting" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    implicit val writer = timeSeriesIndex.getWriter

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i,
            value = 23.5,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag"             -> s"tag_$i"))
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

    val testData = Bit(timestamp = timestamp,
                       value = 0.2,
                       dimensions = Map("dimension" -> s"dimension"),
                       tags = Map("tag"             -> s"tag"))

    timeSeriesIndex.write(testData)

    writer.close()

    val queryExist  = LongPoint.newExactQuery("timestamp", timestamp)
    val resultExist = timeSeriesIndex.query(schema, queryExist, Seq.empty, 100, None)(identity)
    resultExist.size shouldBe 1

    val deleteWriter = timeSeriesIndex.getWriter
    timeSeriesIndex.delete(testData)(deleteWriter)

    deleteWriter.close()

    timeSeriesIndex.refresh()

    val query  = LongPoint.newExactQuery("timestamp", timestamp)
    val result = timeSeriesIndex.query(schema, query, Seq.empty, 100, None)(identity)

    result.size shouldBe 0

  }
}
