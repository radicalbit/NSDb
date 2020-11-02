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
import io.radicalbit.nsdb.common.statement.AvgAggregation
import io.radicalbit.nsdb.common.{NSDbDoubleType, NSDbLongType}
import io.radicalbit.nsdb.index.StorageStrategy.Memory
import io.radicalbit.nsdb.model.{Location, TimeRange}
import io.radicalbit.nsdb.statement.StatementParser.InternalTemporalAggregation
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class AllFacetIndexSpec extends WordSpec with Matchers with OneInstancePerTest {

  "AllFacetIndex" should {
    "Combine Results for temporal average calculation" in {
      val basePath = s"target/test_index/${UUID.randomUUID}"

      val location = Location("metric", "node", 0, 100)
      val directory =
        new MMapDirectory(Paths.get(basePath, "db", "namespace", "shards", s"${location.shardName}"))

      val timeSeriesIndex = new TimeSeriesIndex(directory)

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

      val searcher      = timeSeriesIndex.getSearcher
      val allFacetIndex = new AllFacetIndexes(basePath, "db", "namespace", location, Memory)

      allFacetIndex.executeRangeFacet(searcher,
                                      new MatchAllDocsQuery,
                                      InternalTemporalAggregation(AvgAggregation),
                                      "timestamp",
                                      "value",
                                      Some(BIGINT()),
                                      ranges) shouldBe Seq(
        Bit(10,
            NSDbDoubleType(0.0),
            Map("lowerBound" -> NSDbLongType(0), "upperBound" -> NSDbLongType(10)),
            Map("count"      -> NSDbLongType(10), "sum"       -> NSDbLongType(45))),
        Bit(20,
            NSDbDoubleType(0.0),
            Map("lowerBound" -> NSDbLongType(10), "upperBound" -> NSDbLongType(20)),
            Map("count"      -> NSDbLongType(10), "sum"        -> NSDbLongType(145))),
        Bit(30,
            NSDbDoubleType(0.0),
            Map("lowerBound" -> NSDbLongType(20), "upperBound" -> NSDbLongType(30)),
            Map("count"      -> NSDbLongType(10), "sum"        -> NSDbLongType(245))),
        Bit(40,
            NSDbDoubleType(0.0),
            Map("lowerBound" -> NSDbLongType(30), "upperBound" -> NSDbLongType(40)),
            Map("count"      -> NSDbLongType(10), "sum"        -> NSDbLongType(345))),
        Bit(50,
            NSDbDoubleType(0.0),
            Map("lowerBound" -> NSDbLongType(40), "upperBound" -> NSDbLongType(50)),
            Map("count"      -> NSDbLongType(10), "sum"        -> NSDbLongType(445)))
      )
    }
  }

}
