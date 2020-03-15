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

package io.radicalbit.nsdb.cluster.logic

import io.radicalbit.nsdb.model.Location
import org.scalatest.{Matchers, WordSpec}

class LocalityReadNodesSelectionSpec extends WordSpec with Matchers {

  val localityReadNodesSelection = new LocalityReadNodesSelection("this")

  private def testLocations(node: String, start: Long, end: Long) = (start to end).map { i =>
    Location("metric", node, i, i + 1)
  }

  "LocalityReadNodesSelection" should {
    "privilege local nodes" in {

      val completelyLocalLocations = testLocations("this", 0, 9) ++
        testLocations("that", 0, 9) ++
        testLocations("this", 10, 19) ++
        testLocations("that2", 10, 19)

      val uniqueLocations = localityReadNodesSelection.getUniqueLocationsByNode(completelyLocalLocations)
      uniqueLocations.keySet shouldBe Set("this")
      uniqueLocations("this").sortBy(_.from) shouldBe testLocations("this", 0, 9) ++ testLocations("this", 10, 19)
    }

    "use the minimum amount of other locations when some shards is not locally available" in {

      val scatteredLocations = testLocations("this", 0, 9) ++
        testLocations("that", 0, 9) ++
        testLocations("that", 10, 19) ++
        testLocations("that2", 10, 19) ++
        testLocations("that", 20, 29) ++
        testLocations("that2", 20, 29) ++
        testLocations("that2", 30, 39)

      val uniqueLocations = localityReadNodesSelection.getUniqueLocationsByNode(scatteredLocations)
      uniqueLocations.keySet shouldBe Set("this", "that", "that2")
      uniqueLocations("this").sortBy(_.from) shouldBe testLocations("this", 0, 9)
      uniqueLocations("that").sortBy(_.from) shouldBe testLocations("that", 10, 19) ++ testLocations("that", 20, 29)
      uniqueLocations("that2").sortBy(_.from) shouldBe testLocations("that2", 30, 39)
    }
  }

}
