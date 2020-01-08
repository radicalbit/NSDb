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
import org.apache.lucene.document.LongPoint
import org.apache.lucene.facet.range.{LongRange, LongRangeFacetCounts}
import org.apache.lucene.facet.{FacetResult, FacetsCollector}
import org.apache.lucene.index.Term
import org.apache.lucene.search.{MatchAllDocsQuery, TermQuery}
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class TimeRangeFacetTest extends FlatSpec with Matchers with OneInstancePerTest {

  "TimeSeriesIndex" should "supports facet range query on timestamp without where conditions" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    val records: Seq[Bit] = (0 to 30).map { i =>
      Bit.fromRaw(timestamp = i.toLong,
                  value = i,
                  dimensions = Map("dimension" -> s"dimension_${i / 4}"),
                  tags = Map("tag"             -> s"tag_${i / 4}"))
    }

    implicit val writer = timeSeriesIndex.getWriter
    records.foreach(timeSeriesIndex.write)
    writer.close()

    val ranges: Seq[LongRange] = Seq(
      new LongRange("0-10", 0L, true, 10L, false),
      new LongRange("10-20", 10L, true, 20L, false),
      new LongRange("20-30", 20L, true, 30L, false)
    )

    val fc       = new FacetsCollector
    val searcher = timeSeriesIndex.getSearcher
    val query    = new MatchAllDocsQuery()
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: LongRangeFacetCounts =
      new LongRangeFacetCounts("timestamp", fc, ranges: _*)
    val result: FacetResult = facets.getTopChildren(0, "timestamp")

    result.labelValues.map(_.label).toList shouldBe List("0-10", "10-20", "20-30")
    result.labelValues.map(_.value).toList shouldBe List(10, 10, 10)

  }

  "TimeSeriesIndex" should "supports facet range query on timestamp with where condition on value" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    val records: Seq[Bit] = (0 to 30).map { i =>
      Bit.fromRaw(timestamp = i.toLong,
                  value = i.toLong,
                  dimensions = Map("dimension" -> s"dimension_${i / 4}"),
                  tags = Map("tag"             -> s"tag_${i / 4}"))
    }

    implicit val writer = timeSeriesIndex.getWriter
    records.foreach(timeSeriesIndex.write)
    writer.close()

    val ranges: Seq[LongRange] = Seq(
      new LongRange("0-10", 0L, true, 10L, false),
      new LongRange("10-20", 10L, true, 20L, false),
      new LongRange("20-30", 20L, true, 30L, false)
    )

    val fc       = new FacetsCollector
    val searcher = timeSeriesIndex.getSearcher
    val query    = LongPoint.newRangeQuery("value", 10, Long.MaxValue)
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: LongRangeFacetCounts =
      new LongRangeFacetCounts("timestamp", fc, ranges: _*)
    val result: FacetResult = facets.getTopChildren(0, "timestamp")

    result.labelValues.map(_.label).toList shouldBe List("0-10", "10-20", "20-30")
    result.labelValues.map(_.value).toList shouldBe List(0, 10, 10)

  }

  "TimeSeriesIndex" should "supports facet range query on timestamp with where condition on string dimension" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    val records: Seq[Bit] = (0 to 30).map { i =>
      Bit.fromRaw(timestamp = i.toLong,
                  value = i.toLong,
                  dimensions = Map("dimension" -> s"dimension_${i / 10}"),
                  tags = Map("tag"             -> s"tag_${i / 10}"))
    }

    implicit val writer = timeSeriesIndex.getWriter
    records.foreach(timeSeriesIndex.write)
    writer.close()

    val ranges: Seq[LongRange] = Seq(
      new LongRange("0-10", 0L, true, 10L, false),
      new LongRange("10-20", 10L, true, 20L, false),
      new LongRange("20-30", 20L, true, 30L, false)
    )

    val fc       = new FacetsCollector
    val searcher = timeSeriesIndex.getSearcher
    val query    = new TermQuery(new Term("dimension", "dimension_0"))
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: LongRangeFacetCounts =
      new LongRangeFacetCounts("timestamp", fc, ranges: _*)
    val result: FacetResult = facets.getTopChildren(0, "timestamp")

    result.labelValues.map(_.label).toList shouldBe List("0-10", "10-20", "20-30")
    result.labelValues.map(_.value).toList shouldBe List(10, 0, 0)

  }

  "TimeSeriesIndex" should "supports facet range query on timestamp with where condition on string tag" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    val records: Seq[Bit] = (0 to 30).map { i =>
      Bit.fromRaw(timestamp = i.toLong,
                  value = i.toLong,
                  dimensions = Map("dimension" -> s"dimension_${i / 10}"),
                  tags = Map("tag"             -> s"tag_${i / 10}"))
    }

    implicit val writer = timeSeriesIndex.getWriter
    records.foreach(timeSeriesIndex.write)
    writer.close()

    val ranges: Seq[LongRange] = Seq(
      new LongRange("0-10", 0L, true, 10L, false),
      new LongRange("10-20", 10L, true, 20L, false),
      new LongRange("20-30", 20L, true, 30L, false)
    )

    val fc       = new FacetsCollector
    val searcher = timeSeriesIndex.getSearcher
    val query    = new TermQuery(new Term("tag", "tag_1"))
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: LongRangeFacetCounts =
      new LongRangeFacetCounts("timestamp", fc, ranges: _*)
    val result: FacetResult = facets.getTopChildren(0, "timestamp")

    result.labelValues.map(_.label).toList shouldBe List("0-10", "10-20", "20-30")
    result.labelValues.map(_.value).toList shouldBe List(0, 10, 0)

  }

  "TimeSeriesIndex" should "supports temporal facet aggregation query on timestamp with range on timestamp" in {
    val timeSeriesIndex = new TimeSeriesIndex(new MMapDirectory(Paths.get(s"target/test_index/${UUID.randomUUID}")))

    val records: Seq[Bit] = (0 to 50).map { i =>
      Bit.fromRaw(timestamp = i.toLong,
                  value = i.toLong,
                  dimensions = Map("dimension" -> s"dimension_${i / 10}"),
                  tags = Map("tag"             -> s"tag_${i / 10}"))
    }

    implicit val writer = timeSeriesIndex.getWriter
    records.foreach(timeSeriesIndex.write)
    writer.close()

    val ranges: Seq[LongRange] = Seq(
      new LongRange("0-10", 0L, true, 10L, false),
      new LongRange("10-20", 10L, true, 20L, false),
      new LongRange("20-30", 20L, true, 30L, false),
      new LongRange("30-40", 30L, true, 40L, false),
      new LongRange("40-50", 40L, true, 50L, false)
    )

    val fc       = new FacetsCollector
    val searcher = timeSeriesIndex.getSearcher
    val query    = LongPoint.newRangeQuery("timestamp", 0, 20)
    FacetsCollector.search(searcher, query, 0, fc)
    val facets: LongRangeFacetCounts =
      new LongRangeFacetCounts("timestamp", fc, ranges: _*)
    val result: FacetResult = facets.getTopChildren(0, "timestamp")

    result.labelValues.map(_.label).toList shouldBe List("0-10", "10-20", "20-30", "30-40", "40-50")
    // Range query conditions are inclusive
    result.labelValues.map(_.value).toList shouldBe List(10, 10, 1, 0, 0)

  }

}
