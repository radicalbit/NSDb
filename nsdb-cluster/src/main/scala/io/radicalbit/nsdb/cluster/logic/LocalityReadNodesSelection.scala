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

/**
  * ReadNodesSelection strategy that privileges local locations.
  * @param localNode the local node.
  */
class LocalityReadNodesSelection(localNode: String) extends ReadNodesSelection {

  override def getDistinctLocationsByNode(locationsWithReplicas: Seq[Location]): Map[String, Seq[Location]] = {
    assert(localNode != null)
    locationsWithReplicas
      .groupBy(l => (l.from, l.to))
      .map {
        case ((_, _), locations) if locations.size > 1 =>
          locations.find(_.node == localNode).getOrElse(locations.head)
        case ((_, _), locations) => locations.minBy(_.node)
      }
      .toSeq
      .groupBy(_.node)
  }
}
