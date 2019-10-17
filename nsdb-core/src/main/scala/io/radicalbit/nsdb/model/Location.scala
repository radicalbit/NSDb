/*
 * Copyright 2018 Radicalbit S.r.l.
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

package io.radicalbit.nsdb.model

import spire.implicits._
import spire.math.Interval
import spire.math.interval.Closed

/**
  * Metric shard location.
  *
  * @param metric the metric.
  * @param node a string representation of a node, that is [hostname]_[port].
  * @param from shard interval lower bound.
  * @param to shard interval upper bound.
  */
case class Location(metric: String, node: String, from: Long, to: Long) {
  def shardName = s"${metric}_${from}_$to"

  def interval: Interval[Long] = Interval.fromBounds(Closed(from), Closed(to))
}

object Location {

  /**
    * type alias to enrich a Location with a database and a namespace that contain it.
    */
  type LocationWithCoordinates = (String, String, Location)

  /**
    * @return an empty location
    */
  def empty = Location("", "", 0, 0)
}
