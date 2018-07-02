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
import org.apache.lucene.document.LongPoint
import org.apache.lucene.search.{MatchAllDocsQuery, Sort, SortField}
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{Assertion, FlatSpec, Matchers, OneInstancePerTest}

class FacetIndexTest extends FlatSpec with Matchers with OneInstancePerTest /*with ValidatedMatchers*/ {

  "FacetIndex" should "write and read properly on disk" in {
    val facetIndex = new FacetIndex(
      new MMapDirectory(Paths.get(s"target/test_index/facet/${UUID.randomUUID}")),
      new MMapDirectory(Paths.get(s"target/test_index/facet/taxo,${UUID.randomUUID}"))
    )

    implicit val writer     = facetIndex.getWriter
    implicit val taxoWriter = facetIndex.getTaxoWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(timestamp = System.currentTimeMillis,
            value = 23,
            dimensions = Map("dimension" -> s"dimension_$i"),
            tags = Map("tag"             -> s"tag_$i"))
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }

    val repeatedValue =
      Bit(timestamp = System.currentTimeMillis,
          value = 23,
          dimensions = Map("dimension" -> s"dimension_100"),
          tags = Map("tag"             -> s"tag_100"))
    val w = facetIndex.write(repeatedValue)
    w.isSuccess shouldBe true

    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    assert(fieldName = "dimension", limit = 100, expectedCountSize = 0, expectedSizeDistinct = 0)
    assert(fieldName = "tag", limit = 100, expectedCountSize = 100, expectedSizeDistinct = 100)

    def assert(fieldName: String, limit: Int, expectedCountSize: Int, expectedSizeDistinct: Int): Assertion = {
      val groups   = facetIndex.getCount(new MatchAllDocsQuery(), fieldName, None, Some(limit), VARCHAR())
      val distinct = facetIndex.getDistinctField(new MatchAllDocsQuery(), fieldName, None, limit)

      groups.size shouldBe expectedCountSize
      distinct.size shouldBe expectedSizeDistinct
    }
  }

  "FacetIndex" should "write and read properly on disk with multiple dimensions" in {
    val facetIndex = new FacetIndex(
      new MMapDirectory(Paths.get(s"target/test_index/facet/${UUID.randomUUID}")),
      new MMapDirectory(Paths.get(s"target/test_index/facet/taxo,${UUID.randomUUID}"))
    )

    implicit val writer     = facetIndex.getWriter
    implicit val taxoWriter = facetIndex.getTaxoWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(
          timestamp = System.currentTimeMillis,
          value = 23,
          dimensions = Map("dimension" -> s"dimension_$i", "name" -> s"name_$i"),
          tags = Map("tag"             -> s"tag_$i", "surname"    -> s"surname_$i")
        )
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    assert(fieldName = "dimension", limit = 100, expectedCountSize = 0)
    assert(fieldName = "name", limit = 100, expectedCountSize = 0)

    assert(fieldName = "tag", limit = 100, expectedCountSize = 100)
    assert(fieldName = "surname", limit = 100, expectedCountSize = 100)

    def assert(fieldName: String, limit: Int, expectedCountSize: Int): Assertion = {
      val contentGroups = facetIndex.getCount(new MatchAllDocsQuery(), fieldName, None, Some(limit), VARCHAR())
      contentGroups.size shouldBe expectedCountSize

      val nameGroups = facetIndex.getCount(new MatchAllDocsQuery(), fieldName, None, Some(limit), VARCHAR())
      nameGroups.size shouldBe expectedCountSize
    }
  }

  "FacetIndex" should "write and read properly on disk with multiple dimensions and range query" in {
    val facetIndex = new FacetIndex(
      new MMapDirectory(Paths.get(s"target/test_index/facet/${UUID.randomUUID}")),
      new MMapDirectory(Paths.get(s"target/test_index/facet/taxo,${UUID.randomUUID}"))
    )

    implicit val writer     = facetIndex.getWriter
    implicit val taxoWriter = facetIndex.getTaxoWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i,
            value = 23,
            dimensions = Map("dimension" -> s"dimension_$i", "name" -> s"name_$i"),
            tags = Map("tag"             -> s"tag_$i", "surname"    -> s"surname_$i"))
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    val contentGroups =
      facetIndex.getCount(LongPoint.newRangeQuery("timestamp", 0, 50), "tag", None, Some(100), VARCHAR())
    contentGroups.size shouldBe 50

    assert(fieldName = "name", limit = 100, expectedCountSize = 0)
    assert(fieldName = "surname", limit = 100, expectedCountSize = 100)

    def assert(fieldName: String, limit: Int, expectedCountSize: Int): Assertion = {
      val nameGroups = facetIndex.getCount(new MatchAllDocsQuery(), fieldName, None, Some(limit), VARCHAR())
      nameGroups.size shouldBe expectedCountSize
    }
  }

  "FacetIndex" should "suppport delete" in {
    val facetIndex = new FacetIndex(
      new MMapDirectory(Paths.get(s"target/test_index/facet/${UUID.randomUUID}")),
      new MMapDirectory(Paths.get(s"target/test_index/facet/taxo,${UUID.randomUUID}"))
    )

    implicit val writer     = facetIndex.getWriter
    implicit val taxoWriter = facetIndex.getTaxoWriter

    (1 to 100).foreach { i =>
      val testData =
        Bit(timestamp = i,
            value = 23,
            dimensions = Map("dimension" -> s"dimension_$i", "name" -> s"name_$i"),
            tags = Map("tag"             -> s"tag_$i", "surname"    -> s"surname_$i"))
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    facetIndex.getCount(new MatchAllDocsQuery(), "name", None, Some(100), VARCHAR()).size shouldBe 0
    facetIndex.getCount(new MatchAllDocsQuery(), "surname", None, Some(100), VARCHAR()).size shouldBe 100

    implicit val deleteWriter = facetIndex.getWriter

    facetIndex.delete(
      Bit(timestamp = 100,
          value = 23,
          dimensions = Map("dimension" -> "dimension_100", "name" -> "name_100"),
          tags = Map("tag"             -> "tag_100", "surname"    -> "surname_100")))(deleteWriter)

    deleteWriter.close()
    facetIndex.refresh()

    facetIndex.getCount(new MatchAllDocsQuery(), "surname", None, Some(100), VARCHAR()).size shouldBe 99
  }

  "FacetIndex" should "supports ordering and limiting" in {
    val facetIndex = new FacetIndex(
      new MMapDirectory(Paths.get(s"target/test_index/facet/${UUID.randomUUID}")),
      new MMapDirectory(Paths.get(s"target/test_index/facet/taxo,${UUID.randomUUID}"))
    )

    implicit val writer     = facetIndex.getWriter
    implicit val taxoWriter = facetIndex.getTaxoWriter

    (1 to 100).foreach { i =>
      val factor = i / 4
      val testData =
        Bit(
          timestamp = i,
          value = factor,
          dimensions = Map("dimension" -> s"dimension_$factor", "name" -> s"name_$factor"),
          tags = Map("tag"             -> s"tag_$factor", "surname"    -> s"surname_$factor")
        )
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    val descSort = new Sort(new SortField("value", SortField.Type.INT, true))

    facetIndex
      .getCount(LongPoint.newRangeQuery("timestamp", 0, 50), "dimension", Some(descSort), Some(100), VARCHAR())
      .size shouldBe 0
    facetIndex.getCount(new MatchAllDocsQuery(), "name", None, Some(50), VARCHAR()).size shouldBe 0

    facetIndex
      .getCount(LongPoint.newRangeQuery("timestamp", 0, 50), "tag", Some(descSort), Some(100), VARCHAR())
      .size shouldBe 13
    val surnameGroups = facetIndex.getCount(new MatchAllDocsQuery(), "surname", None, Some(50), VARCHAR())

    surnameGroups.size shouldBe 26

    surnameGroups.head.value shouldBe 4
    surnameGroups.last.value shouldBe 1
  }
}
