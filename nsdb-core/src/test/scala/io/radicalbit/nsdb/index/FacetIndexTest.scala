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

import java.util.UUID

import io.radicalbit.nsdb.actors.ShardKey
import io.radicalbit.nsdb.common.protocol.Bit
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search.{MatchAllDocsQuery, Sort, SortField}
import org.scalatest.{Assertion, FlatSpec, Matchers, OneInstancePerTest}

class FacetIndexTest extends FlatSpec with Matchers with OneInstancePerTest /*with ValidatedMatchers*/ {

  "FacetIndex" should "write and read properly on disk" in {
    val facetIndexes = new AllFacetIndexes(basePath = "target",
                                           db = "test_index",
                                           namespace = "test_facet_index",
                                           key =
                                             ShardKey(metric = UUID.randomUUID.toString, from = 0, to = Long.MaxValue))

    implicit val writer     = facetIndexes.newIndexWriter
    implicit val taxoWriter = facetIndexes.newDirectoryTaxonomyWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(timestamp = System.currentTimeMillis,
            value = 23,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag"             -> s"tag_$i"))
      val w = facetIndexes.write(testData)
      w.isSuccess shouldBe true
    }

    val repeatedValue =
      Bit(timestamp = System.currentTimeMillis,
          value = 23,
          dimensions = Map("dimension" -> s"dimension_100"),
          tags = Map("tag"             -> s"tag_100"))
    val w = facetIndexes.write(repeatedValue)
    w.isSuccess shouldBe true

    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndexes.facetCountIndex.getSearcher

    assert(fieldName = "dimension", limit = 100, expectedCountSize = 0, expectedSizeDistinct = 0)
    assert(fieldName = "tag", limit = 100, expectedCountSize = 100, expectedSizeDistinct = 100)

    def assert(fieldName: String, limit: Int, expectedCountSize: Int, expectedSizeDistinct: Int): Assertion = {
      val groups =
        facetIndexes.facetCountIndex.result(new MatchAllDocsQuery(), fieldName, None, Some(limit), VARCHAR())
      val distinct = facetIndexes.facetCountIndex.getDistinctField(new MatchAllDocsQuery(), fieldName, None, limit)

      groups.size shouldBe expectedCountSize
      distinct.size shouldBe expectedSizeDistinct
    }
  }

  "FacetIndex" should "write and read properly on disk with multiple dimensions" in {

    val facetIndexes = new AllFacetIndexes(basePath = "target",
                                           db = "test_index",
                                           namespace = "test_facet_index",
                                           key =
                                             ShardKey(metric = UUID.randomUUID.toString, from = 0, to = Long.MaxValue))

    implicit val writer     = facetIndexes.newIndexWriter
    implicit val taxoWriter = facetIndexes.newDirectoryTaxonomyWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(
          timestamp = System.currentTimeMillis,
          value = 23,
          dimensions = Map("dimension" -> s"dimension_$i", "name" -> s"name_$i"),
          tags = Map("tag"             -> s"tag_$i", "surname"    -> s"surname_$i")
        )
      val w = facetIndexes.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndexes.facetCountIndex.getSearcher

    assert(fieldName = "dimension", limit = 100, expectedCountSize = 0)
    assert(fieldName = "name", limit = 100, expectedCountSize = 0)

    assert(fieldName = "tag", limit = 100, expectedCountSize = 100)
    assert(fieldName = "surname", limit = 100, expectedCountSize = 100)

    def assert(fieldName: String, limit: Int, expectedCountSize: Int): Assertion = {
      val contentGroups =
        facetIndexes.facetCountIndex.result(new MatchAllDocsQuery(), fieldName, None, Some(limit), VARCHAR())
      contentGroups.size shouldBe expectedCountSize

      val nameGroups =
        facetIndexes.facetCountIndex.result(new MatchAllDocsQuery(), fieldName, None, Some(limit), VARCHAR())
      nameGroups.size shouldBe expectedCountSize
    }
  }

  "FacetIndex" should "write and read properly on disk with multiple dimensions and range query" in {
    val facetIndexes = new AllFacetIndexes(basePath = "target",
                                           db = "test_index",
                                           namespace = "test_facet_index",
                                           key =
                                             ShardKey(metric = UUID.randomUUID.toString, from = 0, to = Long.MaxValue))

    implicit val writer     = facetIndexes.newIndexWriter
    implicit val taxoWriter = facetIndexes.newDirectoryTaxonomyWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i,
            value = 23,
            dimensions = Map("dimension" -> s"dimension_$i", "name" -> s"name_$i"),
            tags = Map("tag"             -> s"tag_$i", "surname"    -> s"surname_$i"))
      val w = facetIndexes.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndexes.facetCountIndex.getSearcher

    val contentGroups =
      facetIndexes.facetCountIndex.result(LongPoint.newRangeQuery("timestamp", 0, 50),
                                          "tag",
                                          None,
                                          Some(100),
                                          VARCHAR())
    contentGroups.size shouldBe 50

    assert(fieldName = "name", limit = 100, expectedCountSize = 0)
    assert(fieldName = "surname", limit = 100, expectedCountSize = 100)

    def assert(fieldName: String, limit: Int, expectedCountSize: Int): Assertion = {
      val nameGroups =
        facetIndexes.facetCountIndex.result(new MatchAllDocsQuery(), fieldName, None, Some(limit), VARCHAR())
      nameGroups.size shouldBe expectedCountSize
    }
  }

  "FacetIndex" should "suppport delete" in {
    val facetIndexes = new AllFacetIndexes(basePath = "target",
                                           db = "test_index",
                                           namespace = "test_facet_index",
                                           key =
                                             ShardKey(metric = UUID.randomUUID.toString, from = 0, to = Long.MaxValue))

    implicit val writer     = facetIndexes.newIndexWriter
    implicit val taxoWriter = facetIndexes.newDirectoryTaxonomyWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i,
            value = 23,
            dimensions = Map("dimension" -> s"dimension_$i", "name" -> s"name_$i"),
            tags = Map("tag"             -> s"tag_$i", "surname"    -> s"surname_$i"))
      val w = facetIndexes.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndexes.facetCountIndex.getSearcher

    facetIndexes.facetCountIndex.result(new MatchAllDocsQuery(), "name", None, Some(100), VARCHAR()).size shouldBe 0
    facetIndexes.facetCountIndex
      .result(new MatchAllDocsQuery(), "surname", None, Some(100), VARCHAR())
      .size shouldBe 100

    val deleteWriter = facetIndexes.newIndexWriter

    facetIndexes.delete(
      Bit(timestamp = 100,
          value = 23,
          dimensions = Map("dimension" -> "dimension_100", "name" -> "name_100"),
          tags = Map("tag"             -> "tag_100", "surname"    -> "surname_100")))(deleteWriter)

    deleteWriter.close()
    facetIndexes.refresh()

    facetIndexes.facetCountIndex
      .result(new MatchAllDocsQuery(), "surname", None, Some(100), VARCHAR())
      .size shouldBe 99
  }

  "FacetIndex" should "supports ordering and limiting on count and sum" in {
    val facetIndexes = new AllFacetIndexes(basePath = "target",
                                           db = "test_index",
                                           namespace = "test_facet_index",
                                           key =
                                             ShardKey(metric = UUID.randomUUID.toString, from = 0, to = Long.MaxValue))

    implicit val writer     = facetIndexes.newIndexWriter
    implicit val taxoWriter = facetIndexes.newDirectoryTaxonomyWriter

    var groups = Map.empty[String, Int]

    (1 to 100).foreach { i =>
      val factor = i / 4
      val tag    = s"tag_$factor"
      val testData =
        Bit(
          timestamp = i,
          value = factor,
          dimensions = Map("dimension" -> s"dimension_$factor", "name" -> s"name_$factor"),
          tags = Map("tag"             -> tag, "surname"               -> s"surname_$factor", "city" -> "Milano")
        )

      facetIndexes.write(testData).isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndexes.facetCountIndex.getSearcher

    val descSort = new Sort(new SortField("value", SortField.Type.INT, true))

    facetIndexes.facetCountIndex
      .result(LongPoint.newRangeQuery("timestamp", 0, 50), "dimension", Some(descSort), Some(100), VARCHAR())
      .size shouldBe 0
    facetIndexes.facetCountIndex.result(new MatchAllDocsQuery(), "name", None, Some(50), VARCHAR()).size shouldBe 0

    facetIndexes.facetCountIndex
      .result(LongPoint.newRangeQuery("timestamp", 0, 50), "tag", Some(descSort), Some(100), VARCHAR())
      .size shouldBe 13

    val res = facetIndexes.facetSumIndex
      .result(LongPoint.newRangeQuery("timestamp", 0, 50), "tag", Some(descSort), Some(100), VARCHAR(), Some(BIGINT()))
    res.size shouldBe 12
    res.foreach {
      case bit =>
        bit.tags.headOption match {
          case Some(("tag", "tag_12")) => 12 * 3 shouldBe bit.value
          case Some(("tag", v: String)) =>
            v.split("_")(1).toInt * 4 shouldBe bit.value
        }
    }

    val surnameGroups =
      facetIndexes.facetCountIndex.result(new MatchAllDocsQuery(), "surname", None, Some(50), VARCHAR())
    surnameGroups.size shouldBe 26
    surnameGroups.head.value shouldBe 4
    surnameGroups.last.value shouldBe 1

    val cityGroups =
      facetIndexes.facetSumIndex.result(new MatchAllDocsQuery(), "city", None, Some(50), VARCHAR(), Some(BIGINT()))
    cityGroups.size shouldBe 1
    cityGroups.head.value shouldBe 1225
  }

  "FacetIndexSum" should "supports a simple sum" in {
    val facetIndexes = new AllFacetIndexes(basePath = "target",
                                           db = "test_index",
                                           namespace = "test_facet_index",
                                           key =
                                             ShardKey(metric = UUID.randomUUID.toString, from = 0, to = Long.MaxValue))

    implicit val writer     = facetIndexes.newIndexWriter
    implicit val taxoWriter = facetIndexes.newDirectoryTaxonomyWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(
          timestamp = i,
          value = 2,
          dimensions = Map("dimension" -> "dimension", "name" -> s"name"),
          tags = Map("tag"             -> "tag", "surname"    -> s"surname")
        )
      val w = facetIndexes.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndexes.facetSumIndex.getSearcher

    val descSort = new Sort(new SortField("value", SortField.Type.INT, true))

    val res1 = facetIndexes.facetSumIndex
      .result(LongPoint.newRangeQuery("timestamp", 0, 100),
              "tag",
              Some(descSort),
              Some(100),
              VARCHAR(),
              Some(BIGINT()))

    res1.size shouldBe 1
    res1.head.value shouldBe 200

    val res2 = facetIndexes.facetSumIndex
      .result(LongPoint.newRangeQuery("timestamp", 0, 50), "tag", Some(descSort), Some(100), VARCHAR(), Some(BIGINT()))
    res2.size shouldBe 1
    res2.head.value shouldBe 100
  }
}
