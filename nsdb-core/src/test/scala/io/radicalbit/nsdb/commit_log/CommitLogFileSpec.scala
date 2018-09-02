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
import java.io.{File, FileOutputStream}
import java.util.UUID

import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.{
  AccumulatedEntry,
  PersistedEntry,
  ReceivedEntry,
  RejectedEntry
}
import io.radicalbit.nsdb.common.protocol.Bit
import org.scalatest.{Matchers, WordSpec}

class CommitLogFileSpec extends WordSpec with Matchers {

  val directory = "target/test_index"

  implicit val serializer = new StandardCommitLogSerializer

  private val separator = System.getProperty("line.separator").toCharArray.head

  def receivedEntry(id: Int)    = ReceivedEntry("db", "namespace", "metric", 0, Bit.empty, id)
  def accumulatedEntry(id: Int) = AccumulatedEntry("db", "namespace", "metric", 0, Bit.empty, id)
  def persistedEntry(id: Int)   = PersistedEntry("db", "namespace", "metric", 0, Bit.empty, id)
  def rejectedEntry(id: Int)    = RejectedEntry("db", "namespace", "metric", 0, Bit.empty, id)

  val balancedEntrySeq = Seq(
    receivedEntry(0),
    accumulatedEntry(0),
    persistedEntry(0),
    receivedEntry(1),
    accumulatedEntry(1),
    persistedEntry(1),
    receivedEntry(2),
    accumulatedEntry(2),
    rejectedEntry(2)
  )

  "CommitLogFile" should {

    "return an empty list if all entries are finalised" in {

      val file   = new File(s"$directory/testCommitLog${UUID.randomUUID().toString}")
      val fileOS = new FileOutputStream(file, true)

      balancedEntrySeq.foreach { entry =>
        fileOS.write(serializer.serialize(entry))
        fileOS.write(separator)
      }

      fileOS.flush()
      fileOS.close()

      import CommitLogFile.CommitLogFile

      file.checkPendingEntries.size shouldBe 0

    }

    "return an nonempty list if some entries are not finalised " in {
      val file   = new File(s"$directory/testCommitLog${UUID.randomUUID().toString}")
      val fileOS = new FileOutputStream(file, true)

      balancedEntrySeq.dropRight(1) foreach { entry =>
        fileOS.write(serializer.serialize(entry))
        fileOS.write(separator)
      }

      fileOS.flush()
      fileOS.close()

      import CommitLogFile.CommitLogFile

      file.checkPendingEntries shouldBe Seq(2)
    }

  }
}
