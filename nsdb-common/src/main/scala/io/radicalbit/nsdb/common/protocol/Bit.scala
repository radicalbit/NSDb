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

package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}

/**
  * Placeholder traits used for serialization purposes
  */
trait NSDbSerializable

/**
  * Class that models NSDb Bit containers hierarchy.
  * A series of time series records (bits) is called metric.
  * NSDb provides two levels of containers for metrics: database and namespace.
  * A bit belongs to a metric, which belongs namespace, which belongs to a database.
  */
case class Coordinates(db: String, namespace: String, metric: String) extends NSDbSerializable

/**
  * Trait that contains Long timestamp field.
  */
trait TimeSeriesRecord {
  val timestamp: Long
}

/**
  * NSDb time series record.
  * A time series record is an object composed of:
  *
  * - a Long timestamp
  *
  * - a numeric value
  *
  * - a Map of generic dimensions (string or numeric values allowed).
  *
  * - a Map of generic tags (string or numeric values allowed).
  *
  * @param timestamp  record timestamp.
  * @param value      record value.
  * @param dimensions record dimensions.
  * @param uniqueValues Set containing all the unique values gathered from a count distinct.
  */
case class Bit(timestamp: Long,
               value: NSDbNumericType,
               dimensions: Map[String, NSDbType],
               tags: Map[String, NSDbType],
               uniqueValues: Set[NSDbType] = Set.empty)
    extends TimeSeriesRecord {

  /**
    * @return all fields included dimensions, timestamp and value.
    */
  def fields: Map[String, (NSDbType, FieldClassType)] =
    extractFields(dimensions, DimensionFieldType) ++
      extractFields(tags, TagFieldType) +
      ("timestamp" -> (NSDbType(timestamp), TimestampFieldType)) +
      ("value"     -> (value, ValueFieldType))

  private def extractFields(m: Map[String, NSDbType],
                            classType: FieldClassType): Map[String, (NSDbType, FieldClassType)] =
    m.map { case (k, v) => k -> (v, classType) }

  /**
    * Compare the bit to another one ignoring the timestamp.
    * @param other the bit to be compared
    * @return true if everything but timestamp is equals
    */
  def timelessEquals(other: Bit): Boolean = {
    this.value == other.value && this.dimensions == other.dimensions && this.tags == other.tags
  }

}

object Bit {
  def empty: Bit = Bit(0, 0L, Map.empty, Map.empty)
}
