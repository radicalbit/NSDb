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

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.test.{NSDbFlatSpec, NSDbTimeSeriesIndexSpecLike}
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.Term
import org.apache.lucene.search._

class TimeSeriesIndexTest extends NSDbFlatSpec with NSDbTimeSeriesIndexSpecLike {

  private val schema = Schema("", Bit(0, 0, Map("dimension" -> "d"), Map("tag" -> "t")))

  "TimeSeriesIndex" should "write and read properly on disk" in {

    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = System.currentTimeMillis,
            value = 23,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag"             -> s"tag_$i"))
      timeSeriesIndex.write(testData)
    }

    commit()

    timeSeriesIndex
      .query(schema, new WildcardQuery(new Term("dimension", "dimension_*")), Seq.empty, 100, None)
      .size shouldBe 100
    timeSeriesIndex.query(schema, new WildcardQuery(new Term("tag", "tag_*")), Seq.empty, 100, None).size shouldBe 100
  }

  "TimeSeriesIndex" should "support values containing dashes" in {
    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i,
            value = 23.5,
            dimensions = Map("dimension" -> s"dimension-$i"),
            tags = Map("tag"             -> s"tag-$i"))
      timeSeriesIndex.write(testData)
    }

    commit()

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
    (0 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i,
            value = 23.5,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag"             -> s"tag_$i"))
      timeSeriesIndex.write(testData)
    }

    commit()

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
    val timestamp = System.currentTimeMillis

    val testData = Bit(timestamp = timestamp,
                       value = 0.2,
                       dimensions = Map("dimension" -> s"dimension"),
                       tags = Map("tag"             -> s"tag"))

    timeSeriesIndex.write(testData)

    commit()

    val queryExist  = LongPoint.newExactQuery("timestamp", timestamp)
    val resultExist = timeSeriesIndex.query(schema, queryExist, Seq.empty, 100, None)
    resultExist.size shouldBe 1

    timeSeriesIndex.delete(testData)

    commit()

    val query  = LongPoint.newExactQuery("timestamp", timestamp)
    val result = timeSeriesIndex.query(schema, query, Seq.empty, 100, None)

    result.size shouldBe 0

  }
}
