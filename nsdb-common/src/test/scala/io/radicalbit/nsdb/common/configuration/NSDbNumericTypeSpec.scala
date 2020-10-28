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

package io.radicalbit.nsdb.common.configuration

import java.math.{MathContext, RoundingMode}

import io.radicalbit.nsdb.common.NSDbNumericType
import io.radicalbit.nsdb.test.NSDbSpec

class NSDbNumericTypeSpec extends NSDbSpec {

  implicit val mathContext: MathContext = new MathContext(10, RoundingMode.HALF_UP)

  "NSDbNumericType" should {

    "support division among integers and doubles" in {

      NSDbNumericType(2) / NSDbNumericType(2.5) shouldBe 0.8

      NSDbNumericType(2.5) / NSDbNumericType(2) shouldBe 1.25

    }

    "use at most the digits set by the math context " in {
      NSDbNumericType(2) / NSDbNumericType(3) shouldBe 0.6666666667

      NSDbNumericType(200) / NSDbNumericType(3) shouldBe 66.66666667
    }

  }

}
