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

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.{NSDbDoubleType, NSDbIntType, NSDbStringType}
import io.radicalbit.nsdb.index.TimeSeriesIndex
import io.radicalbit.nsdb.model.Schema
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class CountDistinctSpec extends WordSpec with Matchers with OneInstancePerTest {

  "TimeSeriesIndex" should {
    "calculate count distinct" in {

      val testRecords: Seq[Bit] = Seq(
        Bit(150000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(160000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(170000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(180000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(120000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(130000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(140000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John")),
        Bit(90000, 5.5, Map("surname"  -> "Doe"), Map("name" -> "John")),
        Bit(60000, 7.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
        Bit(70000, 7.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
        Bit(80000, 8.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
        Bit(30000, 4.5, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
        Bit(0, 1.5, Map("surname"      -> "Doe"), Map("name" -> "Frankie"))
      )

      val timeSeriesIndex =
        new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_first_last_index", UUID.randomUUID().toString)))

      val writer = timeSeriesIndex.getWriter

      testRecords.foreach(timeSeriesIndex.write(_)(writer))

      writer.close()

      val countDistinct =
        timeSeriesIndex.countDistinct(new MatchAllDocsQuery, Schema("testMetric", testRecords.head), "name")

      countDistinct shouldBe Seq(
        Bit(0,
            NSDbIntType(0),
            Map(),
            Map("name" -> NSDbStringType("John")),
            Set(NSDbDoubleType(5.5), NSDbDoubleType(3.5), NSDbDoubleType(2.5))),
        Bit(0,
            NSDbIntType(0),
            Map(),
            Map("name" -> NSDbStringType("Bill")),
            Set(NSDbDoubleType(8.5), NSDbDoubleType(7.5))),
        Bit(0, NSDbIntType(0), Map(), Map("name" -> NSDbStringType("Frank")), Set(NSDbDoubleType(4.5))),
        Bit(0, NSDbIntType(0), Map(), Map("name" -> NSDbStringType("Frankie")), Set(NSDbDoubleType(1.5)))
      )
    }
  }

}
