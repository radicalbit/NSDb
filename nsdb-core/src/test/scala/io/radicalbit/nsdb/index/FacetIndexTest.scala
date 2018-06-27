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
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

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
            dimensions = Map("content" -> s"content_$i"),
            tags = Map.empty)
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }

    val repeatedValue =
      Bit(timestamp = System.currentTimeMillis,
          value = 23,
          dimensions = Map("content" -> s"content_100"),
          tags = Map.empty)
    val w = facetIndex.write(repeatedValue)
    w.isSuccess shouldBe true

    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    val groups   = facetIndex.getCount(new MatchAllDocsQuery(), "content", None, Some(100), VARCHAR())
    val distinct = facetIndex.getDistinctField(new MatchAllDocsQuery(), "content", None, 100)

    groups.size shouldBe 100
    distinct.size shouldBe 100

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
        Bit(timestamp = System.currentTimeMillis,
            value = 23,
            dimensions = Map("content" -> s"content_$i", "name" -> s"name_$i"),
            tags = Map.empty)
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    val contentGroups = facetIndex.getCount(new MatchAllDocsQuery(), "content", None, Some(100), VARCHAR())

    contentGroups.size shouldBe 100

    val nameGroups = facetIndex.getCount(new MatchAllDocsQuery(), "name", None, Some(100), VARCHAR())

    nameGroups.size shouldBe 100
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
            dimensions = Map("content" -> s"content_$i", "name" -> s"name_$i"),
            tags = Map.empty)
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    val contentGroups =
      facetIndex.getCount(LongPoint.newRangeQuery("timestamp", 0, 50), "content", None, Some(100), VARCHAR())

    contentGroups.size shouldBe 50

    val nameGroups = facetIndex.getCount(new MatchAllDocsQuery(), "name", None, Some(100), VARCHAR())

    nameGroups.size shouldBe 100
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
            dimensions = Map("content" -> s"content_$i", "name" -> s"name_$i"),
            tags = Map.empty)
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    val nameGroups = facetIndex.getCount(new MatchAllDocsQuery(), "name", None, Some(100), VARCHAR())

    nameGroups.size shouldBe 100

    implicit val deleteWriter = facetIndex.getWriter

    facetIndex.delete(
      Bit(timestamp = 100,
          value = 23,
          dimensions = Map("content" -> "content_100", "name" -> "name_100"),
          tags = Map.empty))(deleteWriter)

    deleteWriter.close()
    facetIndex.refresh()

    facetIndex.getCount(new MatchAllDocsQuery(), "name", None, Some(100), VARCHAR()).size shouldBe 99
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
        Bit(timestamp = i,
            value = factor,
            dimensions = Map("content" -> s"content_$factor", "name" -> s"name_$factor"),
            tags = Map.empty)
      val w = facetIndex.write(testData)
      w.isSuccess shouldBe true
    }
    taxoWriter.close()
    writer.close()

    implicit val searcher = facetIndex.getSearcher

    val descSort = new Sort(new SortField("value", SortField.Type.INT, true))

    val contentGroups =
      facetIndex.getCount(LongPoint.newRangeQuery("timestamp", 0, 50), "content", Some(descSort), Some(100), VARCHAR())

    contentGroups.size shouldBe 13

    val nameGroups = facetIndex.getCount(new MatchAllDocsQuery(), "name", None, Some(50), VARCHAR())

    nameGroups.size shouldBe 26

    nameGroups.head.value shouldBe 4
    nameGroups.last.value shouldBe 1
  }

}
