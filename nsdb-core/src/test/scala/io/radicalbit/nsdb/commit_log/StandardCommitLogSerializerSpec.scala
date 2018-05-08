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

package io.radicalbit.nsdb.commit_log

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{DeleteEntry, InsertEntry}
import io.radicalbit.nsdb.common.JSerializable
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import org.scalatest.{Matchers, WordSpec}

class StandardCommitLogSerializerSpec extends WordSpec with Matchers {

  "A CommitLogEntry" when {
    val entryService = new StandardCommitLogSerializer

    "has a single dimension" should {
      "be created correctly" in {

        val db        = "test-db"
        val namespace = "test-namespace"
        val ts        = 1500909299161L
        val metric    = "test-metric"
        val bit       = Bit(ts, 0, Map("dimension1" -> "value1"))
        val originalEntry =
          InsertEntry(db = db, namespace = namespace, metric = metric, timestamp = bit.timestamp, bit = bit)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
    }

    "has few dimensions" should {
      "be created correctly" in {

        val db        = "test1-db"
        val namespace = "test1-namespace"
        val ts        = 1500909299165L
        val metric    = "test1-metric"
        val dimensions: Map[String, JSerializable] =
          Map("dimension1" -> "value1", "dimension2" -> 2, "dimension3" -> 3L, "dimension4" -> 3.0)
        val bit = Bit(timestamp = ts, dimensions = dimensions, value = 0)
        val originalEntry =
          InsertEntry(db = db, namespace = namespace, metric = metric, timestamp = bit.timestamp, bit = bit)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
    }
    "for deletion by query" should {
      "be created correctly with RangeExpression for Long" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = RangeExpression("dimension", 10L, 11L)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with RangeExpression for Double" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = RangeExpression("dimension", 10.0, 11.0)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with RangeExpression for Integer" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = RangeExpression("dimension", 10, 11)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with ComparisonExpression for Integer" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = ComparisonExpression("dimension", GreaterOrEqualToOperator, 11)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with ComparisonExpression for Long" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = ComparisonExpression("dimension", GreaterOrEqualToOperator, 11L)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with ComparisonExpression for Double" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = ComparisonExpression("dimension", GreaterOrEqualToOperator, 11.0)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with ComparisonExpression for String" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = ComparisonExpression("dimension", GreaterOrEqualToOperator, "dimValue")
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with EqualityExpression for String" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = EqualityExpression("dimension", "dimValue")
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with EqualityExpression for Integer" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = EqualityExpression("dimension", 1)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with EqualityExpression for Long" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = EqualityExpression("dimension", 1L)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with EqualityExpression for Double" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = EqualityExpression("dimension", 1.0)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with LikeExpression for String" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = LikeExpression("dimension", "v")
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with NullableExpression" in {
        val db         = "test-db"
        val namespace  = "test-namespace"
        val ts         = 1500909299161L
        val metric     = "test-metric"
        val expression = NullableExpression("dimension")
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with UnaryLogicalExpression" in {
        val db        = "test-db"
        val namespace = "test-namespace"
        val ts        = 1500909299161L
        val metric    = "test-metric"
        val expression =
          UnaryLogicalExpression(ComparisonExpression("dimension", GreaterOrEqualToOperator, "dimValue"), NotOperator)
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
      "be created correctly with TupleLogicalExpression" in {
        val db        = "test-db"
        val namespace = "test-namespace"
        val ts        = 1500909299161L
        val metric    = "test-metric"
        val expression =
          TupledLogicalExpression(ComparisonExpression("dimension", GreaterOrEqualToOperator, "dimValue"),
                                  AndOperator,
                                  RangeExpression("timestamp", 10L, 11L))
        val originalEntry =
          DeleteEntry(db = db, namespace = namespace, metric = metric, timestamp = ts, expression = expression)

        val serByteArray = entryService.serialize(originalEntry)
        val desEntry     = entryService.deserialize(serByteArray)

        desEntry should be(originalEntry)
      }
    }
  }

}
