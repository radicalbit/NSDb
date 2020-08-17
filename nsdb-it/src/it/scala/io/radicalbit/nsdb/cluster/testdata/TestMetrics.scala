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
package io.radicalbit.nsdb.cluster.testdata

import io.radicalbit.nsdb.common.protocol.Bit

object TestMetrics {

  object TemporalDoubleMetric {

    val name = "temporalDoubleMetric"

    val testRecords: Seq[Bit] = Seq(
      Bit(150000, 2.5, Map("surname" -> "Doe"), Map("name" -> "John")),
      Bit(120000, 3.5, Map("surname" -> "Doe"), Map("name" -> "John")),
      Bit(90000, 5.5, Map("surname" -> "Doe"), Map("name" -> "John")),
      Bit(60000, 7.5, Map("surname" -> "Doe"), Map("name" -> "Bill")),
      Bit(30000, 4.5, Map("surname" -> "Doe"), Map("name" -> "Frank")),
      Bit(0, 1.5, Map("surname"     -> "Doe"), Map("name" -> "Frankie"))
    )
  }

  object TemporalLongMetric {

    val name = "temporalLongMetric"

    val testRecords: Seq[Bit] = Seq(
      Bit(150000L, 2L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 15L, "height" -> 30.5)),
      Bit(120000L, 3L, Map("surname" -> "Doe"), Map("name" -> "John", "age" -> 20L, "height" -> 30.5)),
      Bit(90000L, 5L, Map("surname" -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
      Bit(60000L, 7L, Map("surname" -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
      Bit(30000L, 4L, Map("surname" -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
      Bit(0L, 1L, Map("surname"     -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
    )
  }

  object LongMetric {

    val name = "longMetric"

    val testRecords: List[Bit] = List(
      Bit(1L, 1L, Map("surname"  -> "Doe"), Map("name" -> "John")),
      Bit(2L, 2L, Map("surname"  -> "Doe"), Map("name" -> "John")),
      Bit(4L, 3L, Map("surname"  -> "D"), Map("name"   -> "J")),
      Bit(6L, 4L, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
      Bit(8L, 5L, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
      Bit(10L, 6L, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
    )

  }

  object DoubleMetric {

    val name = "doubleMetric"

    val testRecords: List[Bit] = List(
      Bit(2L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "John")),
      Bit(4L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "John")),
      Bit(6L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Bill")),
      Bit(8L, 1.5, Map("surname"  -> "Doe"), Map("name" -> "Frank")),
      Bit(10L, 1.5, Map("surname" -> "Doe"), Map("name" -> "Frankie"))
    )

  }

  object AggregationLongMetric {

    val name = "aggregationLongMetric"

    val testRecords: List[Bit] = List(
      Bit(1L, 2L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
      Bit(4L, 4L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 20L, "height" -> 30.5)),
      Bit(2L, 3L, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
      Bit(6L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
      Bit(8L, 1L, Map("surname"  -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
      Bit(10L, 2L, Map("surname" -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
    )

  }

  object AggregationDoubleMetric {

    val name = "aggregationDoubleMetric"

    val testRecords: List[Bit] = List(
      Bit(1L, 2.0, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
      Bit(4L, 4.0, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 20L, "height" -> 30.5)),
      Bit(2L, 3.0, Map("surname"  -> "Doe"), Map("name" -> "John", "age"    -> 15L, "height" -> 30.5)),
      Bit(6L, 1.0, Map("surname"  -> "Doe"), Map("name" -> "Bill", "age"    -> 15L, "height" -> 31.0)),
      Bit(8L, 1.0, Map("surname"  -> "Doe"), Map("name" -> "Frank", "age"   -> 15L, "height" -> 32.0)),
      Bit(10L, 2.0, Map("surname" -> "Doe"), Map("name" -> "Frankie", "age" -> 15L, "height" -> 32.0))
    )

  }

}
