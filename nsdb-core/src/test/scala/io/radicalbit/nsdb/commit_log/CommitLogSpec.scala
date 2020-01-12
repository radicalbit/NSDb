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

package io.radicalbit.nsdb.commit_log
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit

trait CommitLogSpec {

  protected lazy val directory = ConfigFactory.load().getString("nsdb.storage.commit-log-path")

  implicit val serializer = new StandardCommitLogSerializer

  val dummyBit = Bit(0, 10, Map("dim" -> "dim"), Map("tag" -> "tag"))

  val db        = "db"
  val namespace = "namespace"
  val metric    = "metric"

  val bit0 = dummyBit
  val bit1 = dummyBit.copy(timestamp = 1)
  val bit2 = dummyBit.copy(timestamp = 2)

  val id0 = CommitLogBitEntry.bitIdentifier("db", "namespace", "metric", bit0)
  val id1 = CommitLogBitEntry.bitIdentifier("db", "namespace", "metric", bit1)
  val id2 = CommitLogBitEntry.bitIdentifier("db", "namespace", "metric", bit2)

  def receivedEntry(bit: Bit) =
    ReceivedEntry("db",
                  "namespace",
                  "metric",
                  0,
                  bit,
                  CommitLogBitEntry.bitIdentifier("db", "namespace", "metric", bit))
  def accumulatedEntry(bit: Bit) =
    AccumulatedEntry("db",
                     "namespace",
                     "metric",
                     0,
                     bit,
                     CommitLogBitEntry.bitIdentifier("db", "namespace", "metric", bit))
  def persistedEntry(bit: Bit) =
    PersistedEntry("db",
                   "namespace",
                   "metric",
                   0,
                   bit,
                   CommitLogBitEntry.bitIdentifier("db", "namespace", "metric", bit))
  def rejectedEntry(bit: Bit) =
    RejectedEntry("db",
                  "namespace",
                  "metric",
                  0,
                  bit,
                  CommitLogBitEntry.bitIdentifier("db", "namespace", "metric", bit))

  val balancedEntrySeq = Seq(
    receivedEntry(bit0),
    accumulatedEntry(bit0),
    persistedEntry(bit0),
    receivedEntry(bit1),
    accumulatedEntry(bit1),
    persistedEntry(bit1),
    receivedEntry(bit2),
    accumulatedEntry(bit2),
    rejectedEntry(bit2)
  )

  val unbalancedEntrySeq = Seq(
    receivedEntry(bit0),
    accumulatedEntry(bit0),
    persistedEntry(bit0),
    receivedEntry(bit1),
    accumulatedEntry(bit1),
    receivedEntry(bit2),
    accumulatedEntry(bit2),
    rejectedEntry(bit2)
  )

}
