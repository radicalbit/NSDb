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

import io.radicalbit.nsdb.common.NSDbNumericType
import org.scalatest.{Matchers, WordSpec}

class TypeSupportSpec extends WordSpec with Matchers {

  "TypeSupport" when {
    "Index type are used" should {
      "provide right MAX and MIN values for INT" in {
        INT().MAX_VALUE shouldBe NSDbNumericType(Int.MaxValue)
        INT().MIN_VALUE shouldBe NSDbNumericType(Int.MinValue)
      }
      "provide right MAX and MIN values for LONG" in {
        BIGINT().MAX_VALUE shouldBe NSDbNumericType(Long.MaxValue)
        BIGINT().MIN_VALUE shouldBe NSDbNumericType(Long.MinValue)
      }
      "provide right MAX and MIN values for DOUBLE" in {
        DECIMAL().MAX_VALUE shouldBe NSDbNumericType(Double.MaxValue)
        DECIMAL().MIN_VALUE shouldBe NSDbNumericType(Double.MinValue)
      }
    }
  }

}
