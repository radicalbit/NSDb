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

import io.radicalbit.nsdb.test.NSDbSpec
import org.apache.lucene.search.grouping.{LongRange, NSDbLongRangeFactory}

class NSDbLongRangeFactoryTest extends NSDbSpec {

  "NSdbLongRangeFactory" should {
    "throw an exception if values beyond ranges are provided" in {
      val factory = new NSDbLongRangeFactory(0, 50000, 30000)

      an[IllegalArgumentException] should be thrownBy (factory.getRange(30001, null))
      an[IllegalArgumentException] should be thrownBy (factory.getRange(-1, null))
    }

    "calculate the ranges starting from the upperbound" in {
      val factory = new NSDbLongRangeFactory(0, 30000, 50000)

      factory.getRange(50000, null) shouldBe new LongRange(20000, 50000)
      factory.getRange(40000, null) shouldBe new LongRange(20000, 50000)
      factory.getRange(30000, null) shouldBe new LongRange(20000, 50000)
      factory.getRange(20000, null) shouldBe new LongRange(0, 20000)
      factory.getRange(10000, null) shouldBe new LongRange(0, 20000)
      factory.getRange(0, null) shouldBe new LongRange(0, 20000)
    }

    "calculate the ranges when width is an exact divider of the range interval" in {
      val factory = new NSDbLongRangeFactory(120, 30, 180)

      factory.getRange(180, null) shouldBe new LongRange(150, 180)
      factory.getRange(160, null) shouldBe new LongRange(150, 180)
      factory.getRange(150, null) shouldBe new LongRange(120, 150)
      factory.getRange(130, null) shouldBe new LongRange(120, 150)
      factory.getRange(120, null) shouldBe new LongRange(120, 150)
    }

    "calculate the ranges when width is not an exact divider of the range interval" in {
      val factory = new NSDbLongRangeFactory(50, 50, 180)

      factory.getRange(180, null) shouldBe new LongRange(130, 180)
      factory.getRange(160, null) shouldBe new LongRange(130, 180)
      factory.getRange(130, null) shouldBe new LongRange(80, 130)
      factory.getRange(120, null) shouldBe new LongRange(80, 130)
      factory.getRange(100, null) shouldBe new LongRange(80, 130)
      factory.getRange(80, null) shouldBe new LongRange(50, 80)
      factory.getRange(50, null) shouldBe new LongRange(50, 80)
    }
  }

}
