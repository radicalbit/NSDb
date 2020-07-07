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

import io.radicalbit.nsdb.common.NSDbLongType
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.TimeRange
import io.radicalbit.nsdb.statement.StatementParser.InternalCountTemporalAggregation
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.Term
import org.apache.lucene.search.{MatchAllDocsQuery, TermQuery}
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class TimeRangeFacetTest extends WordSpec with Matchers with OneInstancePerTest {

  "FacetRangeIndex" should {
    "supports facet range query on timestamp without where conditions" in {
      val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

      val records: Seq[Bit] = (0 to 30).map { i =>
        Bit(timestamp = i.toLong,
            value = i,
            dimensions = Map("dimension" -> s"dimension_${i / 4}"),
            tags = Map("tag"             -> s"tag_${i / 4}"))
      }

      implicit val writer = timeSeriesIndex.getWriter
      records.foreach(timeSeriesIndex.write)
      writer.close()

      val ranges: Seq[TimeRange] = Seq(
        TimeRange(0L, 10L, true, false),
        TimeRange(10L, 20L, true, false),
        TimeRange(20L, 30L, true, false)
      )

      val searcher        = timeSeriesIndex.getSearcher
      val query           = new MatchAllDocsQuery()
      val facetRangeIndex = new FacetRangeIndex

      facetRangeIndex.executeRangeFacet(searcher,
                                        query,
                                        InternalCountTemporalAggregation,
                                        "timestamp",
                                        "value",
                                        Some(BIGINT()),
                                        ranges) shouldBe Seq(
        Bit(0, NSDbLongType(10), Map("lowerBound"  -> NSDbLongType(0), "upperBound"  -> NSDbLongType(10)), Map()),
        Bit(10, NSDbLongType(10), Map("lowerBound" -> NSDbLongType(10), "upperBound" -> NSDbLongType(20)), Map()),
        Bit(20, NSDbLongType(10), Map("lowerBound" -> NSDbLongType(20), "upperBound" -> NSDbLongType(30)), Map())
      )
    }

    "supports facet range query on timestamp with where condition on value" in {
      val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

      val records: Seq[Bit] = (0 to 30).map { i =>
        Bit(timestamp = i.toLong,
            value = i.toLong,
            dimensions = Map("dimension" -> s"dimension_${i / 4}"),
            tags = Map("tag"             -> s"tag_${i / 4}"))
      }

      implicit val writer = timeSeriesIndex.getWriter
      records.foreach(timeSeriesIndex.write)
      writer.close()

      val ranges: Seq[TimeRange] = Seq(
        TimeRange(0L, 10L, true, false),
        TimeRange(10L, 20L, true, false),
        TimeRange(20L, 30L, true, false)
      )

      val searcher        = timeSeriesIndex.getSearcher
      val query           = LongPoint.newRangeQuery("value", 10, Long.MaxValue)
      val facetRangeIndex = new FacetRangeIndex

      facetRangeIndex.executeRangeFacet(searcher,
                                        query,
                                        InternalCountTemporalAggregation,
                                        "timestamp",
                                        "value",
                                        Some(BIGINT()),
                                        ranges) shouldBe Seq(
        Bit(0, NSDbLongType(0), Map("lowerBound"   -> NSDbLongType(0), "upperBound"  -> NSDbLongType(10)), Map()),
        Bit(10, NSDbLongType(10), Map("lowerBound" -> NSDbLongType(10), "upperBound" -> NSDbLongType(20)), Map()),
        Bit(20, NSDbLongType(10), Map("lowerBound" -> NSDbLongType(20), "upperBound" -> NSDbLongType(30)), Map())
      )
    }

    "supports facet range query on timestamp with where condition on string dimension" in {
      val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

      val records: Seq[Bit] = (0 to 30).map { i =>
        Bit(timestamp = i.toLong,
            value = i.toLong,
            dimensions = Map("dimension" -> s"dimension_${i / 10}"),
            tags = Map("tag"             -> s"tag_${i / 10}"))
      }

      implicit val writer = timeSeriesIndex.getWriter
      records.foreach(timeSeriesIndex.write)
      writer.close()

      val ranges: Seq[TimeRange] = Seq(
        TimeRange(0L, 10L, true, false),
        TimeRange(10L, 20L, true, false),
        TimeRange(20L, 30L, true, false)
      )

      val searcher        = timeSeriesIndex.getSearcher
      val query           = new TermQuery(new Term("dimension", "dimension_0"))
      val facetRangeIndex = new FacetRangeIndex

      facetRangeIndex.executeRangeFacet(searcher,
                                        query,
                                        InternalCountTemporalAggregation,
                                        "timestamp",
                                        "value",
                                        Some(BIGINT()),
                                        ranges) shouldBe Seq(
        Bit(0, NSDbLongType(10), Map("lowerBound" -> NSDbLongType(0), "upperBound"  -> NSDbLongType(10)), Map()),
        Bit(10, NSDbLongType(0), Map("lowerBound" -> NSDbLongType(10), "upperBound" -> NSDbLongType(20)), Map()),
        Bit(20, NSDbLongType(0), Map("lowerBound" -> NSDbLongType(20), "upperBound" -> NSDbLongType(30)), Map())
      )

    }

    "supports facet range query on timestamp with where condition on string tag" in {
      val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

      val records: Seq[Bit] = (0 to 30).map { i =>
        Bit(timestamp = i.toLong,
            value = i.toLong,
            dimensions = Map("dimension" -> s"dimension_${i / 10}"),
            tags = Map("tag"             -> s"tag_${i / 10}"))
      }

      implicit val writer = timeSeriesIndex.getWriter
      records.foreach(timeSeriesIndex.write)
      writer.close()

      val ranges: Seq[TimeRange] = Seq(
        TimeRange(0L, 10L, true, false),
        TimeRange(10L, 20L, true, false),
        TimeRange(20L, 30L, true, false)
      )

      val searcher        = timeSeriesIndex.getSearcher
      val query           = new TermQuery(new Term("tag", "tag_1"))
      val facetRangeIndex = new FacetRangeIndex

      facetRangeIndex.executeRangeFacet(searcher,
                                        query,
                                        InternalCountTemporalAggregation,
                                        "timestamp",
                                        "value",
                                        Some(BIGINT()),
                                        ranges) shouldBe Seq(
        Bit(0, NSDbLongType(0), Map("lowerBound"   -> NSDbLongType(0), "upperBound"  -> NSDbLongType(10)), Map()),
        Bit(10, NSDbLongType(10), Map("lowerBound" -> NSDbLongType(10), "upperBound" -> NSDbLongType(20)), Map()),
        Bit(20, NSDbLongType(0), Map("lowerBound"  -> NSDbLongType(20), "upperBound" -> NSDbLongType(30)), Map())
      )
    }

    "supports temporal facet aggregation query on timestamp with range on timestamp" in {
      val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

      val records: Seq[Bit] = (0 to 50).map { i =>
        Bit(timestamp = i.toLong,
            value = i.toLong,
            dimensions = Map("dimension" -> s"dimension_${i / 10}"),
            tags = Map("tag"             -> s"tag_${i / 10}"))
      }

      implicit val writer = timeSeriesIndex.getWriter
      records.foreach(timeSeriesIndex.write)
      writer.close()

      val ranges: Seq[TimeRange] = Seq(
        TimeRange(0L, 10L, true, false),
        TimeRange(10L, 20L, true, false),
        TimeRange(20L, 30L, true, false),
        TimeRange(30L, 40L, true, false),
        TimeRange(40L, 50L, true, false)
      )

      val searcher        = timeSeriesIndex.getSearcher
      val query           = LongPoint.newRangeQuery("timestamp", 0, 20)
      val facetRangeIndex = new FacetRangeIndex

      facetRangeIndex.executeRangeFacet(searcher,
                                        query,
                                        InternalCountTemporalAggregation,
                                        "timestamp",
                                        "value",
                                        Some(BIGINT()),
                                        ranges) shouldBe Seq(
        Bit(0, NSDbLongType(10), Map("lowerBound"  -> NSDbLongType(0), "upperBound"  -> NSDbLongType(10)), Map()),
        Bit(10, NSDbLongType(10), Map("lowerBound" -> NSDbLongType(10), "upperBound" -> NSDbLongType(20)), Map()),
        Bit(20, NSDbLongType(1), Map("lowerBound"  -> NSDbLongType(20), "upperBound" -> NSDbLongType(30)), Map()),
        Bit(30, NSDbLongType(0), Map("lowerBound"  -> NSDbLongType(30), "upperBound" -> NSDbLongType(40)), Map()),
        Bit(40, NSDbLongType(0), Map("lowerBound"  -> NSDbLongType(40), "upperBound" -> NSDbLongType(50)), Map())
      )
    }
  }

}
