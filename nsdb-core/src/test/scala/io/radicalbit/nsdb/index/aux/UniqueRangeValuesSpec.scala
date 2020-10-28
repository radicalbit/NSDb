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
import io.radicalbit.nsdb.common.{NSDbDoubleType, NSDbStringType}
import io.radicalbit.nsdb.index.TimeSeriesIndex
import io.radicalbit.nsdb.model.{Schema, TimeRangeContext}
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.store.MMapDirectory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class UniqueRangeValuesSpec extends WordSpec with Matchers with OneInstancePerTest {

  val testRecords: Seq[Bit] = Seq(
    Bit(180000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 31L)),
    Bit(170000, 2.5, Map("surname" -> "Doe"), Map("name" -> "Bill", "age" -> 31L)),
    Bit(160000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 30L)),
    Bit(150000, 2.5, Map("surname" -> "Doe"), Map("name" -> "Bill", "age" -> 30L)),
    Bit(140000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 33L)),
    Bit(130000, 3.5, Map("surname" -> "Doe"), Map("name" -> "Bill", "age" -> 33L)),
    Bit(120000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 32L)),
    Bit(90000, 5.5, Map("surname"  -> "Doe"), Map("name" -> "John")),
    Bit(60000, 7.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(70000, 7.5, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age" -> 34L)),
    Bit(80000, 8.5, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age" -> 34L)),
    Bit(30000, 4.5, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(0, 1.5, Map("surname"      -> "Doe"), Map("name" -> "Frankie"))
  )

  "TimeSeriesIndex" should {
    "calculate unique range unique values on the value" in {

      val timeSeriesIndex =
        new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_unique_index", UUID.randomUUID().toString)))

      val writer = timeSeriesIndex.getWriter

      testRecords.foreach(timeSeriesIndex.write(_)(writer))

      writer.close()

       val timeRangeContext = TimeRangeContext(180000, 0 , 30000, Seq.empty)

      val uniqueValues =
        timeSeriesIndex.uniqueRangeValues(new MatchAllDocsQuery, Schema("testMetric", testRecords.head), "value",timeRangeContext)

      uniqueValues shouldBe Seq(
        Bit(0,0,Map("lowerbound" -> 150000L, "upperbound" -> 180000L),Map(),Set(NSDbDoubleType(2.5))),
        Bit(0,0,Map("lowerbound" -> 120000L, "upperbound" -> 150000L),Map(),Set(NSDbDoubleType(3.5))),
        Bit(0,0,Map("lowerbound" -> 90000L, "upperbound" -> 120000L),Map(),Set(NSDbDoubleType(5.5))),
        Bit(0,0,Map("lowerbound" -> 60000L, "upperbound" -> 90000L),Map(),Set(NSDbDoubleType(8.5), NSDbDoubleType(7.5))),
        Bit(0,0,Map("lowerbound" -> 30000L, "upperbound" -> 60000L),Map(),Set(NSDbDoubleType(4.5))),
        Bit(0,0,Map("lowerbound" -> 0L, "upperbound" -> 30000L),Map(),Set(NSDbDoubleType(1.5)))
      )
    }

    "calculate unique range unique values on a tag" in {

      val timeSeriesIndex =
        new TimeSeriesIndex(new MMapDirectory(Paths.get("target", "test_unique_index", UUID.randomUUID().toString)))

      val writer = timeSeriesIndex.getWriter

      testRecords.foreach(timeSeriesIndex.write(_)(writer))

      writer.close()

      val timeRangeContext = TimeRangeContext(180000, 0 , 30000, Seq.empty)

      val uniqueValues =
        timeSeriesIndex.uniqueRangeValues(new MatchAllDocsQuery, Schema("testMetric", testRecords.head), "name",timeRangeContext)

      uniqueValues shouldBe Seq(
        Bit(0,0,Map("lowerbound" -> 150000L, "upperbound" -> 180000L),Map(),Set(NSDbStringType("John"),NSDbStringType("Bill"))),
        Bit(0,0,Map("lowerbound" -> 120000L, "upperbound" -> 150000L),Map(),Set(NSDbStringType("John"),NSDbStringType("Bill"))),
        Bit(0,0,Map("lowerbound" -> 90000L, "upperbound" -> 120000L),Map(),Set(NSDbStringType("John"))),
        Bit(0,0,Map("lowerbound" -> 60000L, "upperbound" -> 90000L),Map(),Set(NSDbStringType("Bill"))),
        Bit(0,0,Map("lowerbound" -> 30000L, "upperbound" -> 60000L),Map(),Set(NSDbStringType("Frank"))),
        Bit(0,0,Map("lowerbound" -> 0L, "upperbound" -> 30000L),Map(),Set(NSDbStringType("Frankie")))
      )
    }

  }

}
