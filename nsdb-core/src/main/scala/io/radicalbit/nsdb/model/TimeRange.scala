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
import spire.math.interval.{Closed, Open}

/**
  * Class that models a range between 2 time instants.
  *
  * @param lowerBound The range's lower bound.
  * @param upperBound The ranges' upper bound.
  * @param lowerInclusive True if the lower bound is inclusive.
  * @param upperInclusive True if the upper bound is inclusive.
  */
case class TimeRange(lowerBound: Long, upperBound: Long, lowerInclusive: Boolean, upperInclusive: Boolean) {

  def interval: Interval[Long] =
    Interval.fromBounds(if (lowerInclusive) Closed(lowerBound) else Open(lowerBound),
                        if (upperInclusive) Closed(upperBound) else Open(upperBound))

  def intersect(location: Location): Boolean = this.interval.intersects(location.interval)
}
