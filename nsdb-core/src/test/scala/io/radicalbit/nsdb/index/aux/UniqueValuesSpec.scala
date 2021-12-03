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

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.{NSDbDoubleType, NSDbIntType, NSDbLongType, NSDbStringType}
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.test.{NSDbSpec, NSDbTimeSeriesIndexSpecLike}
import org.apache.lucene.search.MatchAllDocsQuery

class UniqueValuesSpec extends NSDbSpec with NSDbTimeSeriesIndexSpecLike {

  val testRecords: Seq[Bit] = Seq(
    Bit(150000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 30L)),
    Bit(160000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 30L)),
    Bit(170000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 31L)),
    Bit(180000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 31L)),
    Bit(120000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 32L)),
    Bit(130000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 33L)),
    Bit(140000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 33L)),
    Bit(90000, 5.5, Map("surname"  -> "Doe"), Map("name" -> "John")),
    Bit(60000, 7.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
    Bit(70000, 7.5, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age" -> 34L)),
    Bit(80000, 8.5, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age" -> 34L)),
    Bit(30000, 4.5, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
    Bit(0, 1.5, Map("surname"      -> "Doe"), Map("name" -> "Frankie"))
  )

  "TimeSeriesIndex" should {
    "calculate unique values on value" in {

      testRecords.foreach(timeSeriesIndex.write(_))

      commit()

      val uniqueValues =
        timeSeriesIndex.uniqueValues(new MatchAllDocsQuery, Schema("testMetric", testRecords.head), "name", "value")

      uniqueValues shouldBe Seq(
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

    "calculate unique values on a tag" in {

      testRecords.foreach(timeSeriesIndex.write)

      commit()

      val uniqueValues =
        timeSeriesIndex.uniqueValues(new MatchAllDocsQuery, Schema("testMetric", testRecords.head), "name", "age")

      uniqueValues shouldBe Seq(
        Bit(0,
            NSDbIntType(0),
            Map(),
            Map("name" -> NSDbStringType("John")),
            Set(NSDbLongType(30), NSDbLongType(31), NSDbLongType(32), NSDbLongType(33))),
        Bit(0, NSDbIntType(0), Map(), Map("name" -> NSDbStringType("Bill")), Set(NSDbLongType(34))),
        Bit(0, NSDbIntType(0), Map(), Map("name" -> NSDbStringType("Frank")), Set()),
        Bit(0, NSDbIntType(0), Map(), Map("name" -> NSDbStringType("Frankie")), Set())
      )
    }
  }

}
