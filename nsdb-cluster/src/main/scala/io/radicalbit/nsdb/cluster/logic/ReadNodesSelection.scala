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

import io.radicalbit.nsdb.common.statement.Expression
import io.radicalbit.nsdb.model.{Location, TimeContext}
import io.radicalbit.nsdb.statement.TimeRangeManager
import spire.implicits._
import spire.math.Interval

/**
  * contains the method to select distinct locations
  */
trait ReadNodesSelection {

  /**
    * Returns all the distinct locations grouped by node.
    * @param locationsWithReplicas all the locations for a metric (replicas included).
    */
  def getDistinctLocationsByNode(locationsWithReplicas: Seq[Location]): Map[String, Seq[Location]]

}

object ReadNodesSelection {

  def filterLocationsThroughTime(expression: Option[Expression], locations: Seq[Location]): Seq[Location] = {
    val intervals = TimeRangeManager.extractTimeRange(expression)
    locations.filter {
      case key if intervals.nonEmpty =>
        intervals
          .map(i => Interval.closed(key.from, key.to).intersect(i) != Interval.empty[Long])
          .foldLeft(false)((x, y) => x || y)
      case _ => true
    }
  }

  def filterLocationsThroughGracePeriod(gracePeriod: Long, locations: Seq[Location])(
      implicit timeContext: TimeContext): Seq[Location] = {
    val gracePeriodInterval = Interval.above(timeContext.currentTime - gracePeriod)
    locations.filter { loc =>
      Interval.closed(loc.from, loc.to).intersect(gracePeriodInterval) != Interval.empty[Long]
    }
  }
}
