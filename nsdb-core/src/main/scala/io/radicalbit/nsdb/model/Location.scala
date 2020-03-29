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

package io.radicalbit.nsdb.model

import io.radicalbit.nsdb.common.protocol.NSDbSerializable
import spire.implicits._
import spire.math.Interval
import spire.math.interval.{Closed, Open}

/**
  * Metric shard location.
  *
  * @param metric the metric.
  * @param node a string representation of a node, that is [hostname]_[port].
  * @param from shard interval lower bound.
  * @param to shard interval upper bound.
  */
case class Location(metric: String, node: String, from: Long, to: Long) extends NSDbSerializable {
  def shardName = s"${metric}_${from}_$to"

  def interval: Interval[Long] = Interval.fromBounds(Closed(from), Closed(to))

  /**
    * Checks if the location is beyond retention upper and lower bounds.
    */
  def isBeyond(retention: Long, currentTime: Long = System.currentTimeMillis()): Boolean =
    !interval.intersects(Interval.fromBounds(Open(currentTime - retention), Open(currentTime + retention)))
}

object Location {

  /**
    * @return an empty location
    */
  def empty: Location = Location("", "", 0, 0)
}

/**
  * Enriches a Location with a database and a namespace that contains it.
  */
case class LocationWithCoordinates(db: String, namespace: String, location: Location) extends NSDbSerializable
