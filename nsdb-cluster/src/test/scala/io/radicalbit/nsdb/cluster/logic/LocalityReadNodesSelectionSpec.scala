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

import io.radicalbit.nsdb.common.protocol.NSDbNode
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.test.NSDbSpec

class LocalityReadNodesSelectionSpec extends NSDbSpec {

  private def testLocations(node: NSDbNode, start: Long, end: Long) = (start to end).map { i =>
    Location("metric", node, i, i + 1)
  }

  val thisNode  = NSDbNode("thisNode", "thisNode", "thisVolatile")
  val thatNode  = NSDbNode("thatNode", "thatNode", "thatVolatile")
  val thatNode2 = NSDbNode("thatNode2", "thatNode2", "thatVolatile2")

  val localityReadNodesSelection = new LocalityReadNodesSelection(thisNode.uniqueNodeId)

  "LocalityReadNodesSelection" should {
    "privilege local nodes" in {

      val completelyLocalLocations = testLocations(thisNode, 0, 9) ++
        testLocations(thatNode, 0, 9) ++
        testLocations(thisNode, 10, 19) ++
        testLocations(thatNode2, 10, 19)

      val uniqueLocations = localityReadNodesSelection.getDistinctLocationsByNode(completelyLocalLocations)
      uniqueLocations.keySet shouldBe Set(thisNode.uniqueNodeId)
      uniqueLocations(thisNode.uniqueNodeId)
        .sortBy(_.from) shouldBe testLocations(thisNode, 0, 9) ++ testLocations(thisNode, 10, 19)
    }

    "use the minimum amount of other locations when some shards is not locally available" in {

      val scatteredLocations = testLocations(thisNode, 0, 9) ++
        testLocations(thatNode, 0, 9) ++
        testLocations(thatNode, 10, 19) ++
        testLocations(thatNode2, 10, 19) ++
        testLocations(thatNode, 20, 29) ++
        testLocations(thatNode2, 20, 29) ++
        testLocations(thatNode2, 30, 39)

      val uniqueLocations = localityReadNodesSelection.getDistinctLocationsByNode(scatteredLocations)
      uniqueLocations.keySet shouldBe Set(thisNode.uniqueNodeId, thatNode.uniqueNodeId, thatNode2.uniqueNodeId)
      uniqueLocations(thisNode.uniqueNodeId).sortBy(_.from) shouldBe testLocations(thisNode, 0, 9)
      uniqueLocations(thatNode.uniqueNodeId)
        .sortBy(_.from) shouldBe testLocations(thatNode, 10, 19) ++ testLocations(thatNode, 20, 29)
      uniqueLocations(thatNode2.uniqueNodeId).sortBy(_.from) shouldBe testLocations(thatNode2, 30, 39)
    }
  }

}
